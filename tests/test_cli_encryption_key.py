# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""`extenddb encryption-key` through the CLI on a PostgreSQL catalog with a key ring.

After `switch` the catalog's data_database_connection_string row is encrypted
under the ring's DEK. `status`, `rewrap` and `set-data-connection` open the
ring with the KEK that `[auth.encryption] key_file` names, and
`set-data-connection` replaces a row that does not open. `rewrap` and
`set-data-connection` refuse a catalog whose migrations are not current, as
`migrate` does. Like test_cli_lifecycle.py, this needs a PostgreSQL instance
(EXTENDDB_TEST_PG_CONNECTION_STRING) and a built binary, and is excluded from
the backend-agnostic pytest suite.
"""

import base64
import contextlib
import os
import types

import psycopg2
import pytest

from lifecycle_helpers import PG_ADMIN_CONN, _init_args, _run_extenddb
from test_cli_vector_catalog_migration import _latest

ROW = "data_database_connection_string"
AUTHENTICATED = "Authenticated under the configured KEK."
WRITTEN = f"The {ROW} row was written, encrypted under the DEK of the key ring."
HEALTHY = "=== HEALTHY: All checks passed ==="


@pytest.fixture()
def keyed(cli_env):
    """A catalog after init and `switch --key-file`, with key_file naming the
    KEK, and the helpers that run commands and read rows."""
    config = cli_env["config_path"]

    def run(*args, stdin=None):
        return _run_extenddb(*args, config=config, check=False, stdin=stdin)

    def ok(result, line):
        assert result.returncode == 0 and line in result.stdout, result.stdout + result.stderr

    def private(name, text):
        # A new file that only its owner can read, as under `umask 077`.
        path = os.path.join(os.path.dirname(config), name)
        with open(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as f:
            f.write(text)
        return path

    def kek_file(name):
        # 32 random bytes in standard base64 and one trailing newline, at an
        # absolute path, which key_file requires.
        return private(name, base64.b64encode(os.urandom(32)).decode() + "\n")

    def use_key_file(kek):
        with open(config, "w") as f:
            f.write(f'{generated}\n[auth.encryption]\nkey_file = "{kek}"\n')

    def setting(key):
        with catalog.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
            return cur.fetchone()[0]

    init = _run_extenddb(
        "init", *_init_args(cli_env), config=config, check=False,
        env_override={"EXTENDDB_ADMIN_PASSWORD": "TestPass1!"},
    )
    assert init.returncode == 0, init.stdout + init.stderr
    with open(config) as f:
        generated = f.read()
    kek, kek2 = kek_file("kek"), kek_file("kek2")
    with contextlib.closing(psycopg2.connect(PG_ADMIN_CONN + "/" + cli_env["db_name"])) as catalog:
        catalog.autocommit = True
        # The string init wrote, in plaintext until the switch.
        data_connection = setting(ROW)
        conn = private("conn.txt", data_connection + "\n")

        ok(run("encryption-key", "switch", "--key-file", kek, "--yes"), f"The {ROW} row was rewritten.")
        use_key_file(kek)
        yield types.SimpleNamespace(
            run=run, ok=ok, use_key_file=use_key_file, setting=setting, catalog=catalog,
            kek=kek, kek2=kek2, data_connection=data_connection, conn=conn,
        )


def test_status_rewrap_and_set_data_connection_run_with_the_configured_kek(keyed):
    """init and switch, then status, rewrap and set-data-connection with the
    KEK that key_file names, and set-data-connection on a row that does not
    open."""
    run, ok, setting, kek2 = keyed.run, keyed.ok, keyed.setting, keyed.kek2

    def set_data_connection():
        with open(keyed.conn) as stdin:
            ok(run("encryption-key", "set-data-connection", "--yes", stdin=stdin), WRITTEN)

    ok(run("encryption-key", "status"), AUTHENTICATED)

    ring = setting("encryption_key_ring")
    ok(run("encryption-key", "rewrap", "--key-file", kek2), "Nothing was written.")
    assert setting("encryption_key_ring") == ring
    ok(
        run("encryption-key", "rewrap", "--key-file", kek2, "--yes"),
        "=== extenddb encryption-key rewrap complete ===",
    )
    keyed.use_key_file(kek2)
    ok(run("encryption-key", "status"), AUTHENTICATED)

    set_data_connection()
    ok(run("verify"), HEALTHY)

    # The plaintext row that a psql UPDATE writes does not open under the
    # DEK. status still reports the ring, and set-data-connection replaces
    # the row.
    with keyed.catalog.cursor() as cur:
        cur.execute("UPDATE settings SET value = %s WHERE key = %s", (keyed.data_connection, ROW))
    broken = run("verify")
    assert broken.returncode == 1 and "does not open under the DEK" in broken.stdout, (
        broken.stdout + broken.stderr
    )
    ok(run("encryption-key", "status"), AUTHENTICATED)
    set_data_connection()
    ok(run("verify"), HEALTHY)


# The commands that write beside a server, and the row each writes.
WRITES = [
    (("set-data-connection", "--yes"), ROW),
    (("rewrap", "--key-file", "{kek2}", "--yes"), "encryption_key_ring"),
]


@pytest.mark.parametrize("args,row", WRITES)
def test_a_write_refuses_a_catalog_ahead_by_a_breaking_migration(keyed, args, row):
    """With a breaking migration this binary does not know in the ledger,
    the command exits 4 with the text that says to run `migrate` first, and
    leaves its row as it was."""
    later = _latest() + 1
    with keyed.catalog.cursor() as cur:
        cur.execute(
            "INSERT INTO extenddb_migrations "
            "(id, name, kind, checksum, outcome, applied_by) "
            "VALUES (%s, 'later', 'breaking', 'x', 'applied', 'a later release')",
            (later,),
        )
    before = keyed.setting(row)
    with open(keyed.conn) as stdin:
        refused = keyed.run(
            "encryption-key", *[a.format(kek2=keyed.kek2) for a in args], stdin=stdin
        )
    assert refused.returncode == 4, refused.stdout + refused.stderr
    assert f"{later} later" in refused.stderr, refused.stderr
    assert "run `extenddb migrate` first" in refused.stderr, refused.stderr
    assert keyed.setting(row) == before
