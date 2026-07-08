# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for the persistent GSI propagation queue (``gsi_pending``).

These cover the queue's correctness guarantees end-to-end by driving the real
server lifecycle, so they live alongside the CLI lifecycle tests and reuse their
helpers/fixture:

  * **crash recovery** — committed-but-unpropagated updates survive ``SIGKILL``
    and are applied after restart;
  * **per-key FIFO ordering** — successive updates to one base item converge to
    the latest state with no stale index entries (the partitioned worker model);
  * **dropped-index handling** — rows whose target index table was dropped are
    consumed (savepoint skip), not retried forever.

PostgreSQL-specific (the persistence lives in ``gsi_pending``), gated on
``EXTENDDB_TEST_PG_CONNECTION_STRING``, and excluded from the backend-agnostic
``--pytest`` suite.

CRITICAL (crash recovery): the crash must be ``SIGKILL`` — never a graceful
``extenddb stop``, which drains the queue before exiting and would let the test
pass even with a non-persistent queue (a false positive).
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import time
import uuid

import urllib3

# Reuse the shared CLI-lifecycle infrastructure (lifecycle control, isolated
# per-test deployment, cleanup). The `cli_env` fixture is provided by conftest.
from lifecycle_helpers import (
    PG_ADMIN_CONN,
    _init_args,
    _patch_config_port,
    _run_extenddb,
    _wait_for_server,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ADMIN_PASSWORD = "TestPass1!"
_TLS_CERT = os.path.expanduser("~/.extenddb/tls/cert.pem")
_DEVTOOLS = os.path.join(os.path.dirname(__file__), "..", "devtools")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _set_run_dir(config_path, run_dir):
    """Pin the server's PID-file directory to the test's isolated run_dir."""
    with open(config_path) as f:
        content = f.read()
    if re.search(r"^\s*#?\s*run_dir\s*=", content, re.M):
        content = re.sub(
            r"^\s*#?\s*run_dir\s*=.*$", f'run_dir = "{run_dir}"', content, count=1, flags=re.M
        )
    else:
        content = content.replace(
            'bind_addr = "127.0.0.1"',
            f'bind_addr = "127.0.0.1"\nrun_dir = "{run_dir}"',
            1,
        )
    with open(config_path, "w") as f:
        f.write(content)


def _set_gsi_delay(config_path, ms):
    """Set the system GSI propagation delay (read by the server at startup).

    ``--config`` must precede the ``set`` subcommand for the settings CLI.
    """
    _run_extenddb("settings", "--config", config_path, "set", "gsi_propagation_delay_ms", str(ms))


def _server_pid_on_port(port):
    """PID of the process *listening* on the port (filtered to LISTEN so a
    client connection — including this test — is never returned)."""
    out = subprocess.run(
        ["lsof", "-ti", f"tcp:{port}", "-sTCP:LISTEN"],
        capture_output=True,
        text=True,
        check=False,
    )
    pids = [int(p) for p in out.stdout.split()]
    return pids[0] if pids else None


def _provision_creds(port):
    env = os.environ.copy()
    env.update(
        {
            "EXTENDDB_TEST_ENDPOINT": f"https://127.0.0.1:{port}",
            "EXTENDDB_ADMIN_USER": "admin",
            "EXTENDDB_ADMIN_PASSWORD": ADMIN_PASSWORD,
            "EXTENDDB_CA_CERT": _TLS_CERT,
        }
    )
    res = subprocess.run(
        ["python3", os.path.join(_DEVTOOLS, "provision-test-credentials")],
        capture_output=True,
        text=True,
        env=env,
        check=True,
        timeout=30,
    )
    creds = {}
    for line in res.stdout.splitlines():
        line = line.strip()
        if line.startswith("export "):
            key, _, val = line[len("export ") :].partition("=")
            creds[key] = val.strip().strip('"').strip("'")
    assert "AWS_ACCESS_KEY_ID" in creds and "AWS_SECRET_ACCESS_KEY" in creds, (
        f"provisioning did not return credentials: {res.stdout}"
    )
    return creds


def _ddb_client(port, creds):
    import boto3

    return boto3.client(
        "dynamodb",
        endpoint_url=f"https://127.0.0.1:{port}",
        region_name="us-east-1",
        aws_access_key_id=creds["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=creds["AWS_SECRET_ACCESS_KEY"],
        verify=False,
    )


def _gsi_count(ddb, table, gpk):
    return ddb.query(
        TableName=table,
        IndexName="gsi1",
        KeyConditionExpression="gpk = :g",
        ExpressionAttributeValues={":g": {"S": gpk}},
    )["Count"]


def _data_db_name(cli_env):
    return cli_env["db_name"][: -len("_catalog")]


def _gsi_pending_count(cli_env):
    import psycopg2

    conn = psycopg2.connect(PG_ADMIN_CONN + "/" + _data_db_name(cli_env))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM gsi_pending")
            return cur.fetchone()[0]
    finally:
        conn.close()


def _index_id(cli_env, table_name):
    """Look up the GSI's index_id from the catalog."""
    import psycopg2

    conn = psycopg2.connect(PG_ADMIN_CONN + "/" + cli_env["db_name"])
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT i.index_id FROM indexes i "
                "JOIN tables t ON t.table_id = i.table_id "
                "WHERE t.table_name = %s AND i.index_name = 'gsi1'",
                (table_name,),
            )
            row = cur.fetchone()
            assert row, f"index_id not found for table {table_name}"
            return row[0]
    finally:
        conn.close()


def _drop_index_table(cli_env, index_id):
    import psycopg2

    conn = psycopg2.connect(PG_ADMIN_CONN + "/" + _data_db_name(cli_env))
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "_ddb_{index_id}"')
    finally:
        conn.close()


def _init_serve(cli_env, delay_ms):
    """Init an isolated deployment with the given GSI propagation delay, serve
    it, and return a provisioned DynamoDB client."""
    config = cli_env["config_path"]
    port = cli_env["port"]
    result = _run_extenddb(
        "init",
        *_init_args(cli_env),
        config=config,
        env_override={"EXTENDDB_ADMIN_PASSWORD": ADMIN_PASSWORD},
    )
    assert result.returncode == 0, result.stderr
    _patch_config_port(config, port)
    _set_run_dir(config, cli_env["run_dir"])
    # Set before serving: the server reads the delay at startup.
    _set_gsi_delay(config, delay_ms)
    assert _run_extenddb("serve", config=config).returncode == 0
    assert _wait_for_server(port), "server did not become healthy"
    return _ddb_client(port, _provision_creds(port))


def _create_gsi_table(ddb):
    table = f"gsiq_{uuid.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"},
        ],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.get_waiter("table_exists").wait(TableName=table)
    return table


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
class TestGsiAsyncQueue:
    """Correctness guarantees of the persistent GSI propagation queue."""

    def test_pending_gsi_updates_survive_sigkill(self, cli_env):
        """Committed-but-unpropagated GSI updates survive a hard crash."""
        config = cli_env["config_path"]
        port = cli_env["port"]
        n_items = 5

        ddb = _init_serve(cli_env, 5000)
        table = _create_gsi_table(ddb)

        for i in range(n_items):
            ddb.put_item(
                TableName=table, Item={"pk": {"S": f"pk{i}"}, "gpk": {"S": f"g{i}"}}
            )

        # Genuinely pending now AND still pending after a couple of seconds —
        # proves the long delay took effect (so a pre-crash drain can't mask a
        # non-persistent queue).
        assert _gsi_count(ddb, table, "g0") == 0
        time.sleep(2)
        assert _gsi_count(ddb, table, "g0") == 0, "delay did not take effect"

        # HARD crash (SIGKILL, not `extenddb stop`).
        pid = _server_pid_on_port(port)
        assert pid is not None, "could not locate the running server"
        os.kill(pid, signal.SIGKILL)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and _server_pid_on_port(port) is not None:
            time.sleep(0.2)
        assert _server_pid_on_port(port) is None, "server did not die after SIGKILL"
        for name in os.listdir(cli_env["run_dir"]):
            if name.endswith(".pid"):
                os.remove(os.path.join(cli_env["run_dir"], name))

        # Restart and confirm every queued update was recovered.
        assert _run_extenddb("serve", config=config).returncode == 0
        assert _wait_for_server(port), "server did not restart"
        ddb = _ddb_client(port, _provision_creds(port))

        recovered = {}
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            recovered = {i: _gsi_count(ddb, table, f"g{i}") for i in range(n_items)}
            if all(c == 1 for c in recovered.values()):
                break
            time.sleep(0.5)
        assert all(recovered.get(i) == 1 for i in range(n_items)), (
            f"GSI updates lost across crash: {recovered}"
        )

    def test_same_base_key_updates_apply_in_order(self, cli_env):
        """Successive updates to one base item converge to the latest state with
        no stale index entries — i.e. per-key FIFO ordering holds."""
        ddb = _init_serve(cli_env, 1500)
        table = _create_gsi_table(ddb)

        # Rewrite the same item, changing its GSI key each time. All updates
        # queue (long delay) and must be applied in order by one worker.
        values = [f"v{i}" for i in range(6)]
        for v in values:
            ddb.put_item(TableName=table, Item={"pk": {"S": "X"}, "gpk": {"S": v}})

        latest = values[-1]
        stale = values[:-1]

        # Wait for convergence: the latest key present, the queue drained.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if _gsi_count(ddb, table, latest) == 1 and _gsi_pending_count(cli_env) == 0:
                break
            time.sleep(0.5)

        assert _gsi_count(ddb, table, latest) == 1, "latest GSI entry missing after drain"
        for v in stale:
            assert _gsi_count(ddb, table, v) == 0, (
                f"stale GSI entry for {v} survived — updates applied out of order"
            )
        # And the base item itself reflects the latest write.
        item = ddb.get_item(TableName=table, Key={"pk": {"S": "X"}})["Item"]
        assert item["gpk"]["S"] == latest

    def test_dropped_index_rows_are_consumed(self, cli_env):
        """Rows whose target index table was dropped (table-deletion race) are
        consumed via the savepoint skip, not retried forever."""
        ddb = _init_serve(cli_env, 3000)
        table = _create_gsi_table(ddb)

        for i in range(3):
            ddb.put_item(
                TableName=table, Item={"pk": {"S": f"pk{i}"}, "gpk": {"S": f"g{i}"}}
            )
        assert _gsi_pending_count(cli_env) == 3, "writes were not queued"

        # Drop the index table out from under the pending rows (simulates a
        # table deletion racing with async propagation).
        _drop_index_table(cli_env, _index_id(cli_env, table))

        # The worker must consume the now-unappliable rows (42P01 -> skip),
        # draining the queue rather than looping forever.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if _gsi_pending_count(cli_env) == 0:
                break
            time.sleep(0.5)
        assert _gsi_pending_count(cli_env) == 0, "queue did not drain after index drop"

        # Server is still healthy (not wedged on a poison row).
        assert _wait_for_server(cli_env["port"], timeout=5)

    def test_per_index_delay_overrides_system_default(self, cli_env):
        """A GSI's own propagation delay is honored, not the system default.

        Regression: the enqueue path passed the *system* default delay to every
        pending row, so a per-index ``propagation_delay_ms`` override was
        silently ignored (all async GSIs shared one delay). Here the system
        default is 0 (synchronous); a GSI given its own 5s delay must still
        defer — proving the per-index delay reaches the queue. With the bug the
        write would propagate immediately at the system default.
        """
        import psycopg2

        ddb = _init_serve(cli_env, 0)  # system default: synchronous
        table = _create_gsi_table(ddb)

        # Give gsi1 its own non-zero propagation delay in the catalog.
        conn = psycopg2.connect(PG_ADMIN_CONN + "/" + cli_env["db_name"])
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE indexes SET propagation_delay_ms = 5000 "
                    "FROM tables "
                    "WHERE indexes.table_id = tables.table_id "
                    "AND tables.table_name = %s AND indexes.index_name = 'gsi1'",
                    (table,),
                )
                assert cur.rowcount == 1, "failed to set per-index delay"
        finally:
            conn.close()

        ddb.put_item(TableName=table, Item={"pk": {"S": "X"}, "gpk": {"S": "g0"}})

        # The per-index delay must defer propagation even though the system
        # default is synchronous. With jitter the effective delay is in
        # [delay/2, delay] = [2500ms, 5000ms], so at 1.5s the update is still
        # queued, not yet applied. (With the bug — enqueue using the system
        # default of 0 — it would have applied immediately.)
        time.sleep(1.5)
        assert _gsi_count(ddb, table, "g0") == 0, (
            "per-index delay ignored — update applied at the system default"
        )
        assert _gsi_pending_count(cli_env) == 1, (
            "update was not queued under the per-index delay"
        )

    def test_pending_rows_cleared_on_table_delete(self, cli_env):
        """Deleting a table removes its still-pending ``gsi_pending`` rows in the
        same transaction that drops the tables, so workers never have to
        claim-and-skip orphaned rows after the index tables are gone."""
        ddb = _init_serve(cli_env, 5000)  # long delay: rows stay pending
        table = _create_gsi_table(ddb)

        for i in range(4):
            ddb.put_item(
                TableName=table, Item={"pk": {"S": f"pk{i}"}, "gpk": {"S": f"g{i}"}}
            )
        assert _gsi_pending_count(cli_env) == 4, "writes were not queued"

        ddb.delete_table(TableName=table)
        ddb.get_waiter("table_not_exists").wait(TableName=table)

        assert _gsi_pending_count(cli_env) == 0, (
            "pending GSI rows were left behind after the table was deleted"
        )
