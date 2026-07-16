# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for the SQLite persistent GSI propagation queue (``gsi_pending``).

The SQLite mirror of ``test_gsi_async_queue.py`` (PostgreSQL). Each test drives
the real server lifecycle (``init --backend sqlite`` -> ``serve`` -> optional
``SIGKILL`` -> restart) and inspects/manipulates ``gsi_pending`` and ``indexes``
directly via the SQLite database file. Covers:

  * **crash recovery** — committed-but-unpropagated updates survive ``SIGKILL``
    and are applied after restart;
  * **per-key FIFO ordering** — successive updates to one base item converge to
    the latest state with no stale index entries;
  * **dropped-index handling** — rows whose target index table was dropped are
    consumed, not retried forever;
  * **per-index propagation delay** — a GSI's own ``propagation_delay_ms`` is
    honored over the system default;
  * **orphan cleanup** — a table delete removes that table's pending rows.

SQLite-specific. Self-skips unless the ``extenddb`` binary is a SQLite build
(probed once), so it is inert under the PostgreSQL/agnostic suites.

CRITICAL (crash recovery): the crash must be ``SIGKILL`` — never a graceful
``extenddb stop``, which drains the queue before exiting and would let the test
pass even with a non-persistent queue (a false positive).
"""

from __future__ import annotations

import os
import signal
import sqlite3
import ssl
import subprocess
import time
import urllib.request
import uuid

import boto3
import pytest
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ADMIN_PASSWORD = "TestPass1!"
_DEVTOOLS = os.path.join(os.path.dirname(__file__), "..", "devtools")
EXTENDDB_BINARY = os.environ.get(
    "EXTENDDB_BINARY",
    os.path.join(os.path.dirname(__file__), "..", "target", "release", "extenddb"),
)


# --------------------------------------------------------------------------- #
# Lifecycle helpers
# --------------------------------------------------------------------------- #
def _free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_health(port: int, timeout: float = 20.0) -> bool:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                f"https://127.0.0.1:{port}/health", context=ctx, timeout=2
            ) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.3)
    return False


def _patch_port(config_path: str, port: int) -> None:
    import re

    with open(config_path) as f:
        content = f.read()
    new, n = re.subn(
        r"(?m)^[ \t]*#?[ \t]*port[ \t]*=[ \t]*\d+.*$", f"port = {port}", content, count=1
    )
    if n == 0:
        new = content.replace(
            'bind_addr = "127.0.0.1"', f'bind_addr = "127.0.0.1"\nport = {port}', 1
        )
    with open(config_path, "w") as f:
        f.write(new)


@pytest.fixture(scope="session")
def _sqlite_supported(tmp_path_factory) -> bool:
    """Probe once that the binary is a SQLite build; skip the module otherwise."""
    if not os.path.isfile(EXTENDDB_BINARY):
        pytest.skip(f"extenddb binary not found at {EXTENDDB_BINARY}")
    probe = tmp_path_factory.mktemp("sqlite-probe")
    res = subprocess.run(
        [EXTENDDB_BINARY, "init", "--backend", "sqlite", "--config", "extenddb.toml"],
        cwd=probe,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        timeout=30,
    )
    if res.returncode != 0 or "Unknown backend" in (res.stdout + res.stderr):
        pytest.skip("extenddb binary was not built with the SQLite backend")
    return True


class _Deployment:
    """An isolated, file-backed SQLite deployment under the test's tmp dir."""

    def __init__(self, tmp_dir: str, gsi_delay_ms: int):
        self.dir = tmp_dir
        self.config = os.path.join(tmp_dir, "extenddb.toml")
        self.db_path = os.path.join(tmp_dir, "extenddb.sqlite")
        self.port = _free_port()
        self.proc: subprocess.Popen | None = None
        self._init(gsi_delay_ms)
        self.start()
        self.creds = self._provision()
        self.ddb = self._client()

    def _run(self, *args, check=True, env=None):
        # `--config` goes right after the subcommand: the `settings` CLI requires
        # it before the `set`/`get` action, and it is order-independent elsewhere.
        cmd = [EXTENDDB_BINARY, args[0], "--config", self.config, *args[1:]]
        return subprocess.run(
            cmd,
            cwd=self.dir,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=30,
            check=check,
            env=env,
        )

    def _init(self, gsi_delay_ms: int) -> None:
        env = os.environ.copy()
        env["EXTENDDB_ADMIN_PASSWORD"] = ADMIN_PASSWORD
        out = self._run("init", "--backend", "sqlite", env=env).stdout
        self.admin_password = ADMIN_PASSWORD
        # Best-effort capture if init generated its own password.
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("Password:"):
                self.admin_password = line.split("Password:", 1)[1].strip()
        _patch_port(self.config, self.port)
        # Set the system GSI delay before serving (read at startup).
        self._run("settings", "set", "gsi_propagation_delay_ms", str(gsi_delay_ms))

    def start(self) -> None:
        self.proc = subprocess.Popen(
            [EXTENDDB_BINARY, "serve", "--config", self.config, "--foreground"],
            cwd=self.dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
        )
        assert _wait_for_health(self.port), "server did not become healthy"

    def sigkill(self) -> None:
        assert self.proc is not None
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=10)
        self.proc = None

    def stop(self) -> None:
        if self.proc is not None:
            try:
                self._run("stop", check=False)
            except Exception:
                pass
            try:
                self.proc.send_signal(signal.SIGKILL)
                self.proc.wait(timeout=10)
            except Exception:
                pass
            self.proc = None

    def _provision(self) -> dict:
        env = os.environ.copy()
        env.update(
            {
                "EXTENDDB_TEST_ENDPOINT": f"https://127.0.0.1:{self.port}",
                "EXTENDDB_ADMIN_USER": "admin",
                "EXTENDDB_ADMIN_PASSWORD": self.admin_password,
                "EXTENDDB_CA_CERT": os.path.join(self.dir, ".extenddb", "tls", "cert.pem"),
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
                k, _, v = line[len("export ") :].partition("=")
                creds[k] = v.strip().strip('"').strip("'")
        assert "AWS_ACCESS_KEY_ID" in creds, f"provisioning failed: {res.stdout}\n{res.stderr}"
        return creds

    def _client(self):
        return boto3.client(
            "dynamodb",
            endpoint_url=f"https://127.0.0.1:{self.port}",
            region_name="us-east-1",
            aws_access_key_id=self.creds["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=self.creds["AWS_SECRET_ACCESS_KEY"],
            verify=False,
        )

    # --- direct DB inspection / manipulation (WAL allows concurrent access) ---
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA busy_timeout = 10000")
        return conn

    def gsi_pending_count(self) -> int:
        conn = self._connect()
        try:
            return conn.execute("SELECT count(*) FROM gsi_pending").fetchone()[0]
        finally:
            conn.close()

    def index_id(self, table_name: str, index_name: str = "gsi1") -> str:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT i.index_id FROM indexes i JOIN tables t ON t.table_id = i.table_id "
                "WHERE t.table_name = ? AND i.index_name = ?",
                (table_name, index_name),
            ).fetchone()
            assert row, f"index_id not found for {table_name}.{index_name}"
            return row[0]
        finally:
            conn.close()

    def set_index_delay(self, table_name: str, ms: int, index_name: str = "gsi1") -> None:
        conn = self._connect()
        try:
            cur = conn.execute(
                "UPDATE indexes SET propagation_delay_ms = ? WHERE index_name = ? AND table_id = "
                "(SELECT table_id FROM tables WHERE table_name = ?)",
                (ms, index_name, table_name),
            )
            conn.commit()
            assert cur.rowcount == 1, "failed to set per-index delay"
        finally:
            conn.close()

    def drop_index_table(self, index_id: str) -> None:
        conn = self._connect()
        try:
            conn.execute(f'DROP TABLE IF EXISTS "_ddb_{index_id}"')
            conn.commit()
        finally:
            conn.close()


@pytest.fixture()
def deploy(_sqlite_supported, tmp_path):
    """Factory yielding started SQLite deployments; all are stopped on teardown."""
    created: list[_Deployment] = []

    def _make(gsi_delay_ms: int) -> _Deployment:
        sub = tmp_path / f"d{len(created)}"
        sub.mkdir()
        d = _Deployment(str(sub), gsi_delay_ms)
        created.append(d)
        return d

    yield _make
    for d in created:
        d.stop()


# --------------------------------------------------------------------------- #
# DynamoDB helpers
# --------------------------------------------------------------------------- #
def _create_gsi_table(ddb) -> str:
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


def _gsi_count(ddb, table: str, gpk: str) -> int:
    return ddb.query(
        TableName=table,
        IndexName="gsi1",
        KeyConditionExpression="gpk = :g",
        ExpressionAttributeValues={":g": {"S": gpk}},
    )["Count"]


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
class TestGsiAsyncQueueSqlite:
    """Correctness of the SQLite persistent GSI propagation queue."""

    def test_pending_gsi_updates_survive_sigkill(self, deploy):
        """Committed-but-unpropagated GSI updates survive a hard crash."""
        d = deploy(5000)  # long delay so writes stay pending
        table = _create_gsi_table(d.ddb)
        n_items = 5
        for i in range(n_items):
            d.ddb.put_item(
                TableName=table, Item={"pk": {"S": f"pk{i}"}, "gpk": {"S": f"g{i}"}}
            )

        # Genuinely pending (durably queued, not yet applied).
        assert d.gsi_pending_count() >= 1, "writes were not queued"
        assert _gsi_count(d.ddb, table, "g0") == 0, "delay did not take effect"

        d.sigkill()  # HARD crash — never `extenddb stop`.

        d.start()
        d.ddb = d._client()
        recovered = {}
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            recovered = {i: _gsi_count(d.ddb, table, f"g{i}") for i in range(n_items)}
            if all(c == 1 for c in recovered.values()):
                break
            time.sleep(0.5)
        assert all(recovered.get(i) == 1 for i in range(n_items)), (
            f"GSI updates lost across crash: {recovered}"
        )

    def test_same_base_key_updates_apply_in_order(self, deploy):
        """Successive updates to one base item converge to the latest state."""
        d = deploy(1500)
        table = _create_gsi_table(d.ddb)
        values = [f"v{i}" for i in range(6)]
        for v in values:
            d.ddb.put_item(TableName=table, Item={"pk": {"S": "X"}, "gpk": {"S": v}})

        latest, stale = values[-1], values[:-1]
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if _gsi_count(d.ddb, table, latest) == 1 and d.gsi_pending_count() == 0:
                break
            time.sleep(0.5)
        assert _gsi_count(d.ddb, table, latest) == 1, "latest GSI entry missing after drain"
        for v in stale:
            assert _gsi_count(d.ddb, table, v) == 0, (
                f"stale GSI entry for {v} survived — updates applied out of order"
            )
        item = d.ddb.get_item(TableName=table, Key={"pk": {"S": "X"}})["Item"]
        assert item["gpk"]["S"] == latest

    def test_dropped_index_rows_are_consumed(self, deploy):
        """Rows whose target index table was dropped are consumed, not retried."""
        d = deploy(3000)
        table = _create_gsi_table(d.ddb)
        for i in range(3):
            d.ddb.put_item(
                TableName=table, Item={"pk": {"S": f"pk{i}"}, "gpk": {"S": f"g{i}"}}
            )
        assert d.gsi_pending_count() == 3, "writes were not queued"

        d.drop_index_table(d.index_id(table))

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if d.gsi_pending_count() == 0:
                break
            time.sleep(0.5)
        assert d.gsi_pending_count() == 0, "queue did not drain after index drop"
        assert _wait_for_health(d.port, timeout=5), "server wedged on a poison row"

    def test_per_index_delay_overrides_system_default(self, deploy):
        """A GSI's own propagation delay is honored over the system default."""
        d = deploy(0)  # system default: synchronous
        table = _create_gsi_table(d.ddb)
        d.set_index_delay(table, 5000)  # gsi1 gets its own 5s delay

        d.ddb.put_item(TableName=table, Item={"pk": {"S": "X"}, "gpk": {"S": "g0"}})

        # With jitter the effective delay is in [2500ms, 5000ms]; at 1.5s the
        # update is still queued, not applied. (With the bug — enqueue using the
        # system default of 0 — it would have applied synchronously.)
        time.sleep(1.5)
        assert _gsi_count(d.ddb, table, "g0") == 0, (
            "per-index delay ignored — applied at the system default"
        )
        assert d.gsi_pending_count() == 1, "update was not queued under the per-index delay"

    def test_pending_rows_cleared_on_table_delete(self, deploy):
        """Deleting a table removes its still-pending gsi_pending rows."""
        d = deploy(5000)
        table = _create_gsi_table(d.ddb)
        for i in range(4):
            d.ddb.put_item(
                TableName=table, Item={"pk": {"S": f"pk{i}"}, "gpk": {"S": f"g{i}"}}
            )
        assert d.gsi_pending_count() >= 1, "writes were not queued"

        d.ddb.delete_table(TableName=table)
        d.ddb.get_waiter("table_not_exists").wait(TableName=table)

        assert d.gsi_pending_count() == 0, "pending rows not cleared on table delete"
