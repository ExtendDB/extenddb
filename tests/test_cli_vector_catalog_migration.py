# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Catalog migration tests against a live PostgreSQL deployment.

The vector index schema and the backup table definitions are part of
migration 1, which creates the v1.0 catalog on an empty database and brings a
released catalog to the same schema. These tests load a deployment of a
release from before the vector index schema, from its committed fixture, and
check that the binary refuses to serve it, that `extenddb migrate` upgrades
it, and that the upgraded deployment then serves. They also check that
`--reconverge` runs every migration again on a current catalog, and that a
catalog ledger row for a migration this binary does not carry makes `serve`
refuse.

Shared lifecycle helpers and the `cli_env` fixture live in `lifecycle_helpers.py`.
Like the other CLI lifecycle tests these require a PostgreSQL instance
(EXTENDDB_TEST_PG_CONNECTION_STRING) and a built binary, and are excluded from the
backend-agnostic pytest suite.
"""

from __future__ import annotations

import functools
import os
import re
import subprocess
import time

from lifecycle_helpers import (
    PG_ADMIN_CONN,
    PG_CONN,
    PG_USER,
    _init_args,
    _patch_config_port,
    _pg_args,
    _run_extenddb,
    _wait_for_server,
)

VECTOR_MIGRATION = "002_vector_indexes.sql"
BACKUP_DEFINITIONS_MIGRATION = "003_backup_definitions.sql"

# The last release before the vector index schema (catalog 0.0.2), and the
# directory of the committed fixtures: both databases of each released state
# as that release's own `init`, `serve` and seed steps left them.
PRE_VECTOR_RELEASE = "0.1.10"
RELEASED_FIXTURES = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "crates", "storage-postgres", "tests", "fixtures", "released",
)

# One line of `extenddb migrate --list` per migration: the id, the name, the
# kind, whether it is convergent, and the stores it records rows in.
LISTED = re.compile(
    r"^\s*(\d+)\s+\S+\s+(?:breaking|additive)\s+(?:convergent|not convergent)\s+(.+)$"
)


@functools.cache
def _listed():
    """The stores of every migration the binary carries, by id, from
    `migrate --list`, which opens no database. Adding a migration then needs
    no edit here; a literal id in a test means that migration itself."""
    listed = _run_extenddb("migrate", "--list")
    stores = {}
    for line in listed.stdout.splitlines():
        match = LISTED.match(line)
        if match:
            stores[int(match.group(1))] = [s.strip() for s in match.group(2).split(",")]
    assert stores, listed.stdout
    return stores


def _latest():
    """The highest migration the binary carries, which the runner writes to
    `catalog_version`."""
    return max(_listed())


def _ids_in(store):
    """The ids a complete run records in `store` ("catalog" or "data")."""
    return [i for i, stores in sorted(_listed().items()) if store in stores]


def _conn(database):
    import psycopg2

    conn = psycopg2.connect(PG_ADMIN_CONN + "/" + database)
    conn.autocommit = True
    return conn


def _data_db(cli_env):
    return cli_env["db_name"][: -len("_catalog")]


def _query(database, sql, params=()):
    conn = _conn(database)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]
    finally:
        conn.close()


def _catalog_version(cli_env):
    return _query(
        cli_env["db_name"], "SELECT value FROM settings WHERE key = 'catalog_version'"
    )


def _vector_table_exists(cli_env):
    return _query(cli_env["db_name"], "SELECT to_regclass('public.vector_indexes') IS NOT NULL")


def _backups_has_vector_column(cli_env):
    return _query(
        cli_env["db_name"],
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns "
        "WHERE table_name = 'backups' AND column_name = 'vector_indexes')",
    )


def _ledger_rows(database):
    """`(id, checksum, applied_at)` of every row of a store's ledger."""
    conn = _conn(database)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, checksum, applied_at FROM extenddb_migrations ORDER BY id")
            return cur.fetchall()
    finally:
        conn.close()


def _ledger_ids(database):
    conn = _conn(database)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM extenddb_migrations ORDER BY id")
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def _init(cli_env):
    result = _run_extenddb(
        "init", *_init_args(cli_env),
        config=cli_env["config_path"],
        env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _serve_refused(cli_env):
    """Run `serve --foreground`; it must exit on its own, and the result is
    returned. A server that starts serving is stopped and fails the test."""
    try:
        return _run_extenddb(
            "serve", "--foreground",
            config=cli_env["config_path"],
            check=False,
            timeout=20,
        )
    except subprocess.TimeoutExpired:
        _run_extenddb("stop", config=cli_env["config_path"], check=False)
        raise AssertionError("serve started against a catalog it must refuse") from None


def _load_released_state(cli_env, state):
    """Replace the deployment's two databases with a released state's fixture.

    `extenddb destroy` removes the databases `init` created, keeping the
    config file. They are created again, owned by the application role, and
    each fixture is loaded as that role, as the release's own `init` left it.
    The catalog's connection rows then point at the test's data database.
    """
    import psycopg2

    destroyed = _run_extenddb(
        "destroy", "--yes", *_pg_args(), config=cli_env["config_path"], check=False
    )
    assert destroyed.returncode == 0, destroyed.stdout + destroyed.stderr
    databases = (cli_env["db_name"], _data_db(cli_env))
    admin = psycopg2.connect(PG_ADMIN_CONN + "/postgres")
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            for database in databases:
                cur.execute(f'CREATE DATABASE "{database}" OWNER "{PG_USER}"')
    finally:
        admin.close()
    for database, dump in zip(databases, ("catalog.sql", "data.sql")):
        with open(os.path.join(RELEASED_FIXTURES, state, dump), encoding="utf-8") as f:
            sql = f.read()
        conn = psycopg2.connect(PG_CONN + "/" + database)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
        finally:
            conn.close()
    conn = _conn(cli_env["db_name"])
    try:
        with conn.cursor() as cur:
            for key, value in (
                ("data_database_connection_string", PG_CONN + "/" + _data_db(cli_env)),
                ("data_database_name", _data_db(cli_env)),
            ):
                cur.execute("UPDATE settings SET value = %s WHERE key = %s", (value, key))
    finally:
        conn.close()


class TestVectorCatalogMigration:
    """Migrations from a catalog a release before 1.0 created, on the ledger."""

    def test_init_creates_the_catalog_at_the_current_migration(self, cli_env):
        """A fresh init records every migration in both ledgers and the version.

        `serve` reads the status from the ledgers, so a migration that creates
        its tables without recording them, or the reverse, would leave a
        deployment that cannot serve.
        """
        _init(cli_env)

        assert _catalog_version(cli_env) == str(_latest())
        assert _vector_table_exists(cli_env) is True
        assert _backups_has_vector_column(cli_env) is True
        assert _query(
            cli_env["db_name"], "SELECT to_regclass('public.backup_definitions') IS NOT NULL"
        ) is True
        assert _ledger_ids(cli_env["db_name"]) == list(range(1, _latest() + 1))
        assert _ids_in("catalog") == list(range(1, _latest() + 1))
        data_ids = _ledger_ids(_data_db(cli_env))
        assert data_ids == _ids_in("data")
        # Migrations 1 and 4 have bodies in the data database.
        assert data_ids[:2] == [1, 4], data_ids

        # Migration 1 also writes the legacy ledger rows, so a release before
        # 1.0 started against this catalog finds no legacy file to replay.
        conn = _conn(cli_env["db_name"])
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT filename FROM schema_history ORDER BY filename")
                tracked = {row[0] for row in cur.fetchall()}
        finally:
            conn.close()
        assert VECTOR_MIGRATION in tracked, tracked
        assert BACKUP_DEFINITIONS_MIGRATION in tracked, tracked

    def test_migrate_upgrades_a_catalog_a_release_before_1_0_created(self, cli_env):
        """A v0.1.10 deployment, from before the vector index schema, is
        refused, upgraded by migrate, and then serves.

        The deployment is that release's committed fixture: no migration
        ledger, catalog version 0.0.2, no vector tables. A fresh install with
        its ledger removed is not the same state: it also holds what later
        migrations added, which migration 1's check of its result refuses as
        not part of the v1.0 schema.
        """
        _init(cli_env)
        _patch_config_port(cli_env["config_path"], cli_env["port"])
        _load_released_state(cli_env, PRE_VECTOR_RELEASE)
        assert _vector_table_exists(cli_env) is False

        # The binary refuses a catalog it was not built for (exit 3: migrations
        # pending) rather than starting and failing on the first request that
        # reads the table.
        refused = _serve_refused(cli_env)
        combined = refused.stdout + refused.stderr
        assert refused.returncode == 3, combined
        assert "no migration ledger" in combined, combined
        assert f"needs migration {_latest()}" in combined, combined

        # Without --yes, migrate reports the pending migrations and changes
        # nothing.
        pending = _run_extenddb(
            "migrate", *_pg_args(), config=cli_env["config_path"], check=False
        )
        pending_output = pending.stdout + pending.stderr
        assert pending.returncode == 3, pending_output
        assert "1 baseline (catalog, breaking)" in pending_output, pending_output
        assert "1 baseline (data, breaking)" in pending_output, pending_output
        assert _vector_table_exists(cli_env) is False
        assert _catalog_version(cli_env) == "0.0.2"

        applied = _run_extenddb(
            "migrate", "--yes", *_pg_args(), config=cli_env["config_path"], check=False
        )
        assert applied.returncode == 0, applied.stdout + applied.stderr
        assert f"Catalog: no ledger -> migration {_latest()}" in applied.stdout, applied.stdout
        assert _catalog_version(cli_env) == str(_latest())
        assert _vector_table_exists(cli_env) is True
        assert _backups_has_vector_column(cli_env) is True
        assert _ledger_ids(cli_env["db_name"]) == _ids_in("catalog")
        assert _ledger_ids(_data_db(cli_env)) == _ids_in("data")

        # The upgraded deployment serves.
        served = _run_extenddb("serve", config=cli_env["config_path"])
        assert served.returncode == 0, served.stdout + served.stderr
        try:
            assert _wait_for_server(cli_env["port"]), "upgraded deployment did not serve"
        finally:
            _run_extenddb("stop", config=cli_env["config_path"], check=False)
            time.sleep(1)

        # A second migrate finds every migration recorded.
        again = _run_extenddb(
            "migrate", *_pg_args(), config=cli_env["config_path"], check=False
        )
        again_output = again.stdout + again.stderr
        assert again.returncode == 0, again_output
        assert "up to date" in again_output.lower(), again_output

    def test_reconverge_runs_every_migration_again_on_a_current_catalog(self, cli_env):
        """Every migration is convergent: `migrate --yes --reconverge` runs each
        one again on a catalog at the latest migration and leaves it current,
        with its ledger rows as they were.

        This is the command an operator runs after a release before 1.0 wrote
        to a migrated catalog. Migration 1's check of its result does not run
        again, because a later migration may have changed what it created.
        """
        _init(cli_env)
        stores = (cli_env["db_name"], _data_db(cli_env))
        rows_before = [_ledger_rows(database) for database in stores]

        rerun = _run_extenddb(
            "migrate", "--yes", "--reconverge", *_pg_args(),
            config=cli_env["config_path"],
            check=False,
        )
        output = rerun.stdout + rerun.stderr
        assert rerun.returncode == 0, output
        for i in _ids_in("catalog"):
            assert f"Ran again {i} " in output, output
        assert [_ledger_rows(database) for database in stores] == rows_before
        assert _catalog_version(cli_env) == str(_latest())

        verify = _run_extenddb("verify", config=cli_env["config_path"], check=False)
        assert verify.returncode == 0, verify.stdout + verify.stderr
        assert "OK: Current" in verify.stdout, verify.stdout

    def test_serve_refuses_a_catalog_ahead_by_a_breaking_migration(self, cli_env):
        """A catalog with a breaking migration this binary does not carry.

        The upgrade direction (new binary, old catalog) is covered above. This
        is the other one: a binary rolled back after a breaking migration must
        refuse the catalog (exit 4) rather than write rows in the shape that
        migration replaced. Simulated with a ledger row for the next id, which
        needs no second binary.
        """
        _init(cli_env)
        _patch_config_port(cli_env["config_path"], cli_env["port"])
        later = _latest() + 1
        conn = _conn(cli_env["db_name"])
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO extenddb_migrations "
                    "(id, name, kind, checksum, outcome, applied_by) "
                    "VALUES (%s, 'later', 'breaking', 'x', 'applied', 'a later release')",
                    (later,),
                )
        finally:
            conn.close()

        refused = _serve_refused(cli_env)
        combined = refused.stdout + refused.stderr
        assert refused.returncode == 4, combined
        assert f"{later} later" in combined, combined

        verify = _run_extenddb("verify", config=cli_env["config_path"], check=False)
        assert verify.returncode == 4, verify.stdout + verify.stderr
