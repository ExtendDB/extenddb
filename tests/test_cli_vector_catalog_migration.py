# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Catalog migration tests for the vector index schema (catalog 0.0.2 -> 0.0.3).

These exercise the migration against a live deployment rather than asserting the
SQL text: init one, roll its catalog back to the pre-vector shape, and check that
the binary refuses to serve it, that `extenddb migrate` upgrades it, and that the
upgraded deployment then serves.

Shared lifecycle helpers and the `cli_env` fixture live in `lifecycle_helpers.py`.
Like the other CLI lifecycle tests these require a PostgreSQL instance
(EXTENDDB_TEST_PG_CONNECTION_STRING) and a built binary, and are excluded from the
backend-agnostic pytest suite.
"""

from __future__ import annotations

import subprocess
import time

from lifecycle_helpers import (
    PG_ADMIN_CONN,
    _init_args,
    _patch_config_port,
    _pg_args,
    _run_extenddb,
    _wait_for_server,
)

VECTOR_MIGRATION = "002_vector_indexes.sql"


def _catalog_conn(cli_env):
    import psycopg2

    return psycopg2.connect(PG_ADMIN_CONN + "/" + cli_env["db_name"])


def _catalog_query(cli_env, sql):
    conn = _catalog_conn(cli_env)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]
    finally:
        conn.close()


def _catalog_version(cli_env):
    return _catalog_query(
        cli_env, "SELECT value FROM settings WHERE key = 'catalog_version'"
    )


def _vector_table_exists(cli_env):
    return _catalog_query(
        cli_env, "SELECT to_regclass('public.vector_indexes') IS NOT NULL"
    )


def _backups_has_vector_column(cli_env):
    return _catalog_query(
        cli_env,
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns "
        "WHERE table_name = 'backups' AND column_name = 'vector_indexes')",
    )


def _init(cli_env):
    result = _run_extenddb(
        "init", *_init_args(cli_env),
        config=cli_env["config_path"],
        env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _roll_catalog_back_to_pre_vector(cli_env):
    """Turn a freshly initialised catalog into the shape 0.0.2 deployments have.

    Reproduces an upgrade rather than a fresh install, which is the case that
    matters: a fresh install applies both migrations in order and reaches the same
    end state trivially.
    """
    conn = _catalog_conn(cli_env)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS vector_indexes")
            cur.execute("ALTER TABLE backups DROP COLUMN IF EXISTS vector_indexes")
            cur.execute(
                "DELETE FROM schema_history WHERE filename = %s", (VECTOR_MIGRATION,)
            )
            cur.execute("UPDATE settings SET value = '0.0.2' WHERE key = 'catalog_version'")
    finally:
        conn.close()


class TestVectorCatalogMigration:
    """Catalog version 0.0.3: the vector index table and the backup snapshot."""

    def test_init_creates_the_vector_catalog_at_0_0_3(self, cli_env):
        """A fresh init applies both catalog migrations and records the version.

        The version is what the binary checks at startup, so a migration that
        creates the table without moving the version, or the reverse, would leave
        a deployment that cannot serve.
        """
        _init(cli_env)

        assert _catalog_version(cli_env) == "0.0.3"
        assert _vector_table_exists(cli_env) is True
        assert _backups_has_vector_column(cli_env) is True

        conn = _catalog_conn(cli_env)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT filename FROM schema_history ORDER BY filename")
                tracked = {row[0] for row in cur.fetchall()}
        finally:
            conn.close()
        # Recorded, so a later migrate does not walk the file list again on a
        # deployment that is already current. The statements are idempotent too,
        # so a replay after a crash between applying and recording is harmless.
        assert VECTOR_MIGRATION in tracked, tracked

    def test_migrate_upgrades_a_pre_vector_deployment(self, cli_env):
        """A 0.0.2 deployment is refused, upgraded by migrate, and then serves."""
        _init(cli_env)
        _patch_config_port(cli_env["config_path"], cli_env["port"])
        _roll_catalog_back_to_pre_vector(cli_env)
        assert _vector_table_exists(cli_env) is False

        # The binary must refuse to serve a catalog it was not built for, rather
        # than starting and failing on the first request that reads the table.
        try:
            refused_serve = _run_extenddb(
                "serve", "--foreground",
                config=cli_env["config_path"],
                check=False,
                timeout=20,
            )
        except subprocess.TimeoutExpired:
            _run_extenddb("stop", config=cli_env["config_path"], check=False)
            raise AssertionError(
                "serve started against a 0.0.2 catalog; the version gate did not hold"
            ) from None
        combined = refused_serve.stdout + refused_serve.stderr
        assert refused_serve.returncode != 0, combined
        assert "0.0.3" in combined and "0.0.2" in combined, combined

        # Without --yes, migrate reports the pending upgrade and changes nothing.
        pending = _run_extenddb(
            "migrate", *_pg_args(), config=cli_env["config_path"], check=False
        )
        pending_output = pending.stdout + pending.stderr
        assert pending.returncode != 0, pending_output
        assert "0.0.2 -> 0.0.3" in pending_output, pending_output
        assert _vector_table_exists(cli_env) is False
        assert _catalog_version(cli_env) == "0.0.2"

        applied = _run_extenddb(
            "migrate", "--yes", *_pg_args(), config=cli_env["config_path"], check=False
        )
        assert applied.returncode == 0, applied.stdout + applied.stderr
        assert _catalog_version(cli_env) == "0.0.3"
        assert _vector_table_exists(cli_env) is True
        assert _backups_has_vector_column(cli_env) is True

        # The upgraded deployment serves, which is the assertion the version and
        # the table shape are both really for.
        served = _run_extenddb("serve", config=cli_env["config_path"])
        assert served.returncode == 0, served.stdout + served.stderr
        try:
            assert _wait_for_server(cli_env["port"]), "upgraded deployment did not serve"
        finally:
            _run_extenddb("stop", config=cli_env["config_path"], check=False)
            time.sleep(1)

        # A second migrate is a no-op: the ledger row stops the non-idempotent
        # migration from running twice.
        again = _run_extenddb(
            "migrate", *_pg_args(), config=cli_env["config_path"], check=False
        )
        again_output = again.stdout + again.stderr
        assert again.returncode == 0, again_output
        assert "up to date" in again_output.lower(), again_output

    def test_migrate_survives_an_applied_but_unrecorded_migration(self, cli_env):
        """A replay of the vector migration must succeed, not block the deployment.

        The runner applies a migration and records it in `schema_history` as two
        separate commits, so a crash in between leaves the schema applied and the
        ledger short a row. That state is silent at first, because the catalog
        version moved with the schema and the startup gate is satisfied. It bites
        when the next catalog migration lands: the runner then walks every file,
        finds this one unrecorded, and applies it a second time.

        This reproduces that second run. The version is set to a value that makes
        the runner walk the list, with the vector schema still present and its
        ledger row missing, which is exactly the state a crash leaves behind.
        Without idempotent statements the replay fails on "relation already
        exists" and no later migration can ever be applied.
        """
        _init(cli_env)
        assert _vector_table_exists(cli_env) is True

        conn = _catalog_conn(cli_env)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Ledger row gone, schema left in place: the crash window.
                cur.execute(
                    "DELETE FROM schema_history WHERE filename = %s", (VECTOR_MIGRATION,)
                )
                # Force the runner to walk the file list, the way a later
                # migration would.
                cur.execute(
                    "UPDATE settings SET value = '0.0.2' WHERE key = 'catalog_version'"
                )
        finally:
            conn.close()

        replayed = _run_extenddb(
            "migrate", "--yes", *_pg_args(), config=cli_env["config_path"], check=False
        )
        output = replayed.stdout + replayed.stderr
        assert replayed.returncode == 0, output
        # The file was re-applied rather than skipped by the ledger, which is what
        # makes this a replay and not a no-op.
        assert f"Applying {VECTOR_MIGRATION}" in output, output
        # PostgreSQL emits "already exists, skipping" notices here, which is the
        # guards working. What must not appear is the runner's failure line.
        assert f"Migration {VECTOR_MIGRATION} failed" not in output, output

        # The replay leaves the same end state, and the ledger is repaired.
        assert _catalog_version(cli_env) == "0.0.3"
        assert _vector_table_exists(cli_env) is True
        assert _backups_has_vector_column(cli_env) is True
        conn = _catalog_conn(cli_env)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM schema_history WHERE filename = %s",
                    (VECTOR_MIGRATION,),
                )
                assert cur.fetchone()[0] == 1
        finally:
            conn.close()

    def test_serve_refuses_a_catalog_newer_than_the_binary(self, cli_env):
        """The version gate must refuse a catalog from a future release too.

        The upgrade direction (new binary, old catalog) is covered above. This is
        the other one, which matters during a rolling upgrade: a replica still
        running the old build must refuse a catalog a newer replica has already
        migrated, rather than serving against a schema it does not understand.
        The check is an exact-equality comparison, so both directions come from
        the same line, and pinning them both is what keeps that true.

        Simulated by moving the stored version forward, which needs no second
        binary.
        """
        _init(cli_env)
        _patch_config_port(cli_env["config_path"], cli_env["port"])

        conn = _catalog_conn(cli_env)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE settings SET value = '0.0.4' WHERE key = 'catalog_version'"
                )
        finally:
            conn.close()

        try:
            refused = _run_extenddb(
                "serve", "--foreground",
                config=cli_env["config_path"],
                check=False,
                timeout=20,
            )
        except subprocess.TimeoutExpired:
            _run_extenddb("stop", config=cli_env["config_path"], check=False)
            raise AssertionError(
                "serve started against a 0.0.4 catalog; the version gate is not symmetric"
            ) from None
        combined = refused.stdout + refused.stderr
        assert refused.returncode != 0, combined
        assert "0.0.3" in combined and "0.0.4" in combined, combined
