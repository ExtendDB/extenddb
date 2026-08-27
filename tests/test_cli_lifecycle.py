# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""CLI lifecycle tests for extenddb (init -> serve -> status -> stop -> destroy).

Shared lifecycle helpers and the `cli_env` fixture live in `lifecycle_helpers.py`; the
fixture is re-exported via `conftest.py` for auto-discovery. These require a
PostgreSQL instance (EXTENDDB_TEST_PG_CONNECTION_STRING) and a built binary, and
are excluded from the backend-agnostic pytest suite.
"""

import os
import time
import uuid

import pytest

from lifecycle_helpers import (
    PG_ADMIN_CONN,
    PG_ADMIN_PASS,
    PG_ADMIN_USER,
    PG_CONN,
    _drop_database,
    _fail_if_no_binary,
    _fail_if_no_pg,
    _find_free_port,
    _init_args,
    _patch_config_port,
    _pg_args,
    _run_extenddb,
    _wait_for_server,
)

class TestCliLifecycle:
    """Test the full extenddb CLI lifecycle."""

    def test_version(self):
        """extenddb version prints version info."""
        _fail_if_no_binary()
        result = _run_extenddb("version")
        assert "extenddb" in result.stdout.lower()

    def test_init_creates_schema(self, cli_env):
        """extenddb init creates the catalog schema and TLS certs."""
        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        assert result.returncode == 0

        # TLS certs should exist
        assert os.path.isfile(os.path.join(cli_env["tls_dir"], "cert.pem"))
        assert os.path.isfile(os.path.join(cli_env["tls_dir"], "key.pem"))

    def test_data_migrations_tracked(self, cli_env):
        """init records every data migration in the data DB's _sqlx_migrations.

        Validates the sqlx data-migration ledger: migrations are tracked (so
        `extenddb migrate` never re-runs an applied one), and the GSI-pending
        migration is among them.
        """
        import psycopg2

        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        assert result.returncode == 0

        # The data database is the catalog name without the "_catalog" suffix.
        data_db = cli_env["db_name"][: -len("_catalog")]
        conn = psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT version FROM _sqlx_migrations ORDER BY version")
                tracked = {row[0] for row in cur.fetchall()}
                cur.execute("SELECT name FROM code_migrations ORDER BY name")
                code_tracked = {row[0] for row in cur.fetchall()}
        finally:
            conn.close()

        # 001_data_schema, 002_gsi_pending, 003_idempotency_account_scope.
        assert {1, 2, 3} <= tracked, tracked
        # The programmatic (code) migration for the GSI base-key index is
        # tracked in its own code_migrations ledger (it is Rust code, not a
        # checksummed SQL file), applied and skipped by `migrate` just like
        # the SQL migrations.
        assert "003_gsi_base_key_index" in code_tracked, code_tracked

    def test_migrate_applies_pending_data_migration(self, cli_env):
        """`extenddb migrate` applies a pending *data* migration on an existing
        deployment — not just catalog migrations.

        Regression: data migrations live in the data database's own
        sqlx ledger (`_sqlx_migrations`), independent of the catalog version. `migrate`
        previously only ran catalog migrations and returned "up to date" when
        the catalog version matched, so a release that only added a data
        migration (e.g. `002_gsi_pending.sql`) was never applied on upgrade —
        the `gsi_pending` table was missing and every async-GSI write failed.

        Simulates a pre-002 deployment by dropping `gsi_pending` and its
        ledger row, then asserts `migrate` re-applies it (and refuses without
        `--yes`).
        """
        import psycopg2

        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        assert result.returncode == 0

        data_db = cli_env["db_name"][: -len("_catalog")]

        def _data_conn():
            return psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)

        # Simulate a deployment created before 002 existed.
        conn = _data_conn()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS gsi_pending")
                cur.execute("DELETE FROM _sqlx_migrations WHERE version = 2")
                cur.execute("SELECT to_regclass('public.gsi_pending') IS NOT NULL")
                assert cur.fetchone()[0] is False
        finally:
            conn.close()

        # Without --yes, migrate must refuse while a data migration is pending.
        refused = _run_extenddb(
            "migrate", *_pg_args(),
            config=cli_env["config_path"],
            check=False,
        )
        assert refused.returncode != 0, refused.stdout + refused.stderr

        # With --yes, migrate applies the pending data migration.
        applied = _run_extenddb(
            "migrate", "--yes", *_pg_args(),
            config=cli_env["config_path"],
            check=False,
        )
        assert applied.returncode == 0, applied.stdout + applied.stderr

        conn = _data_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.gsi_pending') IS NOT NULL")
                assert cur.fetchone()[0] is True
                cur.execute(
                    "SELECT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version = 2)"
                )
                assert cur.fetchone()[0] is True
                # The 002 schema must carry the per-index queue columns.
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'gsi_pending'"
                )
                cols = {row[0] for row in cur.fetchall()}
                assert {"worker_partition", "ready_at", "index_context"} <= cols, cols
        finally:
            conn.close()

    def test_migrate_applies_base_key_index_code_migration(self, cli_env):
        """`extenddb migrate` applies the programmatic GSI base-key-index
        migration (`003_gsi_base_key_index`) and never re-runs it.

        The base-key index makes GSI-propagation deletes (`WHERE base_pk = $1
        AND base_sk_* = $2`) index scans instead of sequential scans. New tables
        get it at creation time; this migration adds it to tables created before
        the index existed. It is a *code* migration (it enumerates the
        dynamically-named `_ddb_*` tables and uses `CREATE INDEX CONCURRENTLY`),
        so this verifies it is wired into the same pending/apply/tracking flow as
        the SQL migrations: pending is detected, `migrate` refuses without
        `--yes`, applies with `--yes`, records the ledger row, and is idempotent
        on a second run. (Index derivation itself is the same logic exercised at
        table-creation time across the GSI integration suite.)
        """
        import psycopg2

        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        assert result.returncode == 0

        data_db = cli_env["db_name"][: -len("_catalog")]

        def _data_conn():
            return psycopg2.connect(PG_ADMIN_CONN + "/" + data_db)

        # Simulate a deployment created before 003 existed: drop only its ledger
        # row so `migrate` sees the code migration as pending again.
        conn = _data_conn()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM code_migrations "
                    "WHERE name = '003_gsi_base_key_index'"
                )
                cur.execute(
                    "SELECT EXISTS(SELECT 1 FROM code_migrations "
                    "WHERE name = '003_gsi_base_key_index')"
                )
                assert cur.fetchone()[0] is False
        finally:
            conn.close()

        # Without --yes, migrate must refuse while the code migration is pending.
        refused = _run_extenddb(
            "migrate", *_pg_args(),
            config=cli_env["config_path"],
            check=False,
        )
        assert refused.returncode != 0, refused.stdout + refused.stderr
        assert "003_gsi_base_key_index" in (refused.stdout + refused.stderr)

        # With --yes, migrate applies and records the code migration.
        applied = _run_extenddb(
            "migrate", "--yes", *_pg_args(),
            config=cli_env["config_path"],
            check=False,
        )
        assert applied.returncode == 0, applied.stdout + applied.stderr

        conn = _data_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT EXISTS(SELECT 1 FROM code_migrations "
                    "WHERE name = '003_gsi_base_key_index')"
                )
                assert cur.fetchone()[0] is True
        finally:
            conn.close()

        # Idempotent: a second migrate finds nothing pending.
        again = _run_extenddb(
            "migrate", "--yes", *_pg_args(),
            config=cli_env["config_path"],
            check=False,
        )
        assert again.returncode == 0, again.stdout + again.stderr
        assert "up to date" in again.stdout.lower(), again.stdout

    def test_migrate_refuses_pre_sqlx_catalog(self, cli_env):
        """`extenddb migrate` refuses a catalog created by the pre-sqlx runner.

        ADR-0003 adopts sqlx with a re-init upgrade path (no in-place shim). A
        catalog that predates sqlx (has `schema_history`, no `_sqlx_migrations`)
        must not be migrated in place: `migrate` refuses and directs the operator
        to `destroy` + `init`, rather than failing later on a non-idempotent DDL
        re-run and leaving the catalog half-adopted.
        """
        import psycopg2

        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        assert result.returncode == 0

        # Rewrite the catalog to look like a pre-sqlx deployment: legacy
        # schema-history table, no sqlx ledger, old catalog version.
        conn = psycopg2.connect(PG_ADMIN_CONN + "/" + cli_env["db_name"])
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS _sqlx_migrations")
                cur.execute(
                    "CREATE TABLE schema_history (filename TEXT PRIMARY KEY)"
                )
                cur.execute(
                    "UPDATE settings SET value = '0.0.2' WHERE key = 'catalog_version'"
                )
        finally:
            conn.close()

        refused = _run_extenddb(
            "migrate", "--yes", *_pg_args(),
            config=cli_env["config_path"],
            check=False,
        )
        assert refused.returncode != 0, refused.stdout + refused.stderr
        combined = (refused.stdout + refused.stderr).lower()
        assert "predates" in combined, combined
        assert "destroy" in combined and "init" in combined, combined

        # The version must be left untouched (no in-place stamp), and the guard
        # must have fired before the migrator connected: no sqlx ledger created.
        conn = psycopg2.connect(PG_ADMIN_CONN + "/" + cli_env["db_name"])
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM settings WHERE key = 'catalog_version'")
                assert cur.fetchone()[0] == "0.0.2"
                cur.execute("SELECT to_regclass('public._sqlx_migrations') IS NULL")
                assert cur.fetchone()[0] is True
        finally:
            conn.close()

    def test_init_serve_status_stop(self, cli_env):
        """Full lifecycle: init → serve → status → stop."""
        # Init
        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        assert result.returncode == 0
        _patch_config_port(cli_env["config_path"], cli_env["port"])

        # Serve (daemonizes)
        result = _run_extenddb("serve", config=cli_env["config_path"])
        assert result.returncode == 0

        # Wait for server to be healthy
        assert _wait_for_server(cli_env["port"]), "Server did not become healthy"

        # Status should report running
        result = _run_extenddb("status", config=cli_env["config_path"])
        assert result.returncode == 0
        assert "running" in result.stdout.lower() or "pid" in result.stdout.lower()

        # Stop
        result = _run_extenddb("stop", config=cli_env["config_path"])
        assert result.returncode == 0

        # Status should report not running
        time.sleep(1)
        result = _run_extenddb("status", config=cli_env["config_path"], check=False)
        # Status returns non-zero when not running
        assert result.returncode != 0 or "not running" in result.stdout.lower()

    def test_init_serve_stop_destroy(self, cli_env):
        """Full lifecycle including destroy."""
        # Init
        _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        _patch_config_port(cli_env["config_path"], cli_env["port"])

        # Serve
        _run_extenddb("serve", config=cli_env["config_path"])
        assert _wait_for_server(cli_env["port"])

        # Stop
        _run_extenddb("stop", config=cli_env["config_path"])
        time.sleep(1)

        # Destroy
        result = _run_extenddb("destroy", "--yes", *_pg_args(), config=cli_env["config_path"])
        assert result.returncode == 0

    def test_serve_without_init_fails(self, cli_env):
        """extenddb serve without init should fail."""
        result = _run_extenddb("serve", config=cli_env["config_path"], check=False)
        assert result.returncode != 0

    def test_destroy_without_yes_fails(self, cli_env):
        """extenddb destroy without --yes should fail."""
        _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        result = _run_extenddb("destroy", config=cli_env["config_path"], check=False)
        assert result.returncode != 0

    def test_double_init_fails(self, cli_env):
        """extenddb init on an already-initialized database should fail."""
        _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            check=False,
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        # Second init should fail (catalog already exists)
        assert result.returncode != 0

    def test_stop_when_not_running(self, cli_env):
        """extenddb stop when no server is running should handle gracefully."""
        _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        result = _run_extenddb("stop", config=cli_env["config_path"], check=False)
        # Should exit non-zero or report not running
        # (exact behavior depends on implementation)
        assert result.returncode != 0 or "not running" in result.stdout.lower()

    def test_init_with_unix_socket(self, cli_env):
        """extenddb init with Unix socket path generates valid connection string and daemon starts."""
        import platform
        import psycopg2

        # Only run on Linux or macOS
        if platform.system() not in ("Linux", "Darwin"):
            pytest.skip(f"Unix socket test only runs on Linux/macOS, not {platform.system()}")

        # Try to find PostgreSQL's Unix socket by connecting without host
        # (psycopg2 defaults to Unix socket on Linux/macOS)
        socket_path = None
        try:
            # Connect using Unix socket to discover the socket directory
            conn = psycopg2.connect(
                dbname="postgres",
                user=PG_ADMIN_USER,
                password=PG_ADMIN_PASS,
            )
            # Query the socket directory from PostgreSQL
            with conn.cursor() as cur:
                cur.execute("SHOW unix_socket_directories")
                socket_dirs = cur.fetchone()[0]
                # Take the first directory if multiple are listed
                socket_path = socket_dirs.split(",")[0].strip()
            conn.close()

            # Verify the socket directory exists and is accessible
            if not os.path.isdir(socket_path):
                socket_path = None
        except Exception as e:
            # If Unix socket connection fails, PostgreSQL might not be configured for it
            # This is expected and we skip the test rather than fail
            pytest.skip(f"PostgreSQL Unix socket not available: {e}")

        if not socket_path:
            pytest.skip("PostgreSQL Unix socket directory not found")

        # Override pg_host with Unix socket path
        cli_env["pg_host"] = socket_path

        # Init with Unix socket
        result = _run_extenddb(
            "init", *_init_args(cli_env),
            config=cli_env["config_path"],
            env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
        )
        assert result.returncode == 0, f"Init failed: {result.stderr}"

        # Verify config contains percent-encoded socket path
        with open(cli_env["config_path"]) as f:
            config_content = f.read()

        # Socket path should be percent-encoded in connection string
        encoded_path = socket_path.replace("/", "%2F")
        assert encoded_path in config_content, (
            f"Connection string should contain percent-encoded socket path {encoded_path}"
        )

        # Verify daemon can start with the generated config
        _patch_config_port(cli_env["config_path"], cli_env["port"])
        result = _run_extenddb("serve", config=cli_env["config_path"])
        assert result.returncode == 0, f"Serve failed: {result.stderr}"

        # Wait for server to be healthy
        assert _wait_for_server(cli_env["port"]), "Server did not become healthy with Unix socket connection"

        # Stop
        _run_extenddb("stop", config=cli_env["config_path"])


class TestCliMultiInstance:
    """Test multi-instance isolation — two extenddb instances on different ports/databases."""

    def test_two_instances_isolated(self, tmp_path):
        """Two extenddb instances with different configs don't interfere."""
        _fail_if_no_pg()
        _fail_if_no_binary()

        from urllib.parse import urlparse
        parsed = urlparse(PG_CONN)
        pg_host = parsed.hostname or "localhost"
        pg_port = str(parsed.port or 5432)

        instances = []
        for i in range(2):
            db_name = f"extenddb_multi_{uuid.uuid4().hex[:8]}_catalog"
            port = _find_free_port()
            inst_dir = tmp_path / f"inst{i}"
            os.makedirs(str(inst_dir), exist_ok=True)

            config_path = str(inst_dir / "extenddb.toml")

            instances.append(
                {
                    "db_name": db_name,
                    "config_path": config_path,
                    "port": port,
                    "pg_host": pg_host,
                    "pg_port": pg_port,
                }
            )

        try:
            # Init both
            for inst in instances:
                result = _run_extenddb(
                    "init", *_init_args(inst),
                    config=inst["config_path"],
                    env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
                )
                assert result.returncode == 0
                _patch_config_port(inst["config_path"], inst["port"])

            # Serve both
            for inst in instances:
                result = _run_extenddb("serve", config=inst["config_path"])
                assert result.returncode == 0

            # Both should be healthy
            for inst in instances:
                assert _wait_for_server(inst["port"]), (
                    f"Instance on port {inst['port']} did not become healthy"
                )

            # Both should report running
            for inst in instances:
                result = _run_extenddb("status", config=inst["config_path"])
                assert result.returncode == 0

        finally:
            # Cleanup: stop and destroy both
            for inst in instances:
                try:
                    _run_extenddb(
                        "stop", config=inst["config_path"], check=False, timeout=10
                    )
                except Exception:
                    pass
                try:
                    _run_extenddb(
                        "destroy",
                        "--yes",
                        *_pg_args(),
                        config=inst["config_path"],
                        check=False,
                        timeout=10,
                    )
                except Exception:
                    pass
                _drop_database(inst["db_name"])
                if inst["db_name"].endswith("_catalog"):
                    _drop_database(inst["db_name"][:-len("_catalog")])
