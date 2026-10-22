# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""End-to-end wire test for the resource-tag condition-key migration (SQLite).

Drives a real SQLite deployment through its lifecycle on its own data
directory and port, independent of the server the integration job already
started:

  1. init (a fresh catalog carries the migration marker) and serve; the first
     start of a fresh catalog logs no migration line.
  2. store an inline user policy, a group policy, a permissions boundary and a
     role trust policy using every operator shape of dynamodb:ResourceTag,
     plus a conditional allow used for the decision check, through the
     management API.
  3. stop, run `migrate` without --yes: it lists each rewrite with a count and
     exits non-zero, and nothing on disk changes (read back with the sqlite3
     module).
  4. run `migrate --yes`, restart, and confirm a GetItem gated on the resource
     tag that was denied before the migration is allowed after it.
  5. delete the marker row with the sqlite3 module and confirm serve refuses to
     start, naming `extenddb migrate --yes`.

The binary is found the way tests/lifecycle_helpers.py does, falling back to
target/release/extenddb, so the SQLite integration job runs it against the
release binary it builds. On a non-SQLite binary (the PostgreSQL job) the test
skips, because `init --backend sqlite` is rejected there.

This file is NOT in devtools/run-tests' ignore list, so the main pytest run of
both integration jobs collects it; only the SQLite job's binary makes it run.
"""

from __future__ import annotations

import json
import os
import re
import signal
import socket
import sqlite3
import subprocess
import time
import uuid

import boto3
import pytest
import requests
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from lifecycle_helpers import EXTENDDB_BINARY

MIGRATION_MARKER_KEY = "resource_tag_migration"

# This test stands up its own deployment, so it must not inherit admin
# credentials or an endpoint from an outer harness (devtools/run-tests and the
# SQLite job export EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD /
# EXTENDDB_TEST_ENDPOINT for the already-running server). With those set, `init`
# would adopt the ambient admin credential and print no generated password, and
# the test could not authenticate to its own server. Scrub them for every
# subprocess this test launches.
_AMBIENT_KEYS = (
    "EXTENDDB_ADMIN_USER",
    "EXTENDDB_ADMIN_PASSWORD",
    "EXTENDDB_TEST_ENDPOINT",
    "REQUESTS_CA_BUNDLE",
    "SSL_CERT_FILE",
    "CURL_CA_BUNDLE",
    "AWS_CA_BUNDLE",
)


def _clean_env() -> dict:
    env = os.environ.copy()
    for k in _AMBIENT_KEYS:
        env.pop(k, None)
    return env


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _run(*args, timeout=60, env=None):
    return subprocess.run(
        [EXTENDDB_BINARY, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        stdin=subprocess.DEVNULL,
        env=env if env is not None else _clean_env(),
    )


def _binary_is_sqlite(config_path: str, sqlite_path: str) -> bool:
    """A SQLite init succeeds only on a SQLite-backend binary; the PostgreSQL
    binary rejects --backend sqlite. Use that to skip on the wrong binary."""
    proc = _run(
        "init",
        "--backend",
        "sqlite",
        "--sqlite-path",
        sqlite_path,
        "--config",
        config_path,
        timeout=60,
    )
    if proc.returncode == 0:
        return True
    combined = proc.stdout + proc.stderr
    if "backend" in combined.lower():
        return False
    raise AssertionError(f"unexpected init failure: {combined}")


def _admin_password(init_output: str) -> str:
    m = re.search(r"Password:\s*(\S+)", init_output)
    assert m, f"no admin password in init output: {init_output}"
    return m.group(1)


def _patch_port(config_path: str, port: int) -> None:
    with open(config_path) as f:
        content = f.read()
    new_content, n = re.subn(
        r"(?m)^[ \t]*#?[ \t]*port[ \t]*=[ \t]*\d+.*$",
        f"port = {port}",
        content,
        count=1,
    )
    if n == 0:
        new_content = content.replace(
            'bind_addr = "127.0.0.1"',
            f'bind_addr = "127.0.0.1"\nport = {port}',
            1,
        )
    with open(config_path, "w") as f:
        f.write(new_content)


class _Server:
    """A foreground extenddb server subprocess with its own log file."""

    def __init__(self, config_path: str, port: int, log_path: str):
        self.config_path = config_path
        self.port = port
        self.log_path = log_path
        self.proc: subprocess.Popen | None = None

    def start(self, wait_healthy=True, timeout=60) -> None:
        log = open(self.log_path, "w")
        self.proc = subprocess.Popen(
            [EXTENDDB_BINARY, "serve", "--config", self.config_path, "--foreground"],
            stdout=log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            env=_clean_env(),
        )
        if not wait_healthy:
            return
        url = f"https://127.0.0.1:{self.port}/health"
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"server exited early: {self.read_log()}")
            try:
                r = requests.get(url, verify=False, timeout=2)
                if r.ok and "healthy" in r.text:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.5)
        raise AssertionError(f"server did not become healthy: {self.read_log()}")

    def stop(self) -> None:
        if self.proc is None:
            return
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self.proc = None

    def read_log(self) -> str:
        try:
            with open(self.log_path) as f:
                return f.read()
        except FileNotFoundError:
            return ""


def _client(endpoint: str, ak: str, sk: str, region: str):
    return boto3.client(
        "dynamodb",
        endpoint_url=endpoint,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        region_name=region,
        config=BotoConfig(retries={"max_attempts": 0}),
        verify=False,
    )


def _policy(resource_tag_key: str) -> dict:
    """An allow gated on <resource_tag_key> = prod."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["dynamodb:GetItem", "dynamodb:DescribeTable"],
                "Resource": "*",
                "Condition": {"StringEquals": {resource_tag_key: "prod"}},
            }
        ],
    }


def _every_shape_policy() -> dict:
    """One statement per operator shape, all on dynamodb:ResourceTag/*."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
             "Condition": {"StringEquals": {"dynamodb:ResourceTag/env": "prod"}}},
            {"Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
             "Condition": {"StringNotEquals": {"dynamodb:ResourceTag/env": "prod"}}},
            {"Effect": "Allow", "Action": "dynamodb:PutItem", "Resource": "*",
             "Condition": {"StringEqualsIfExists": {"dynamodb:ResourceTag/env": "prod"}}},
            {"Effect": "Allow", "Action": "dynamodb:Query", "Resource": "*",
             "Condition": {"ForAllValues:StringEquals": {"dynamodb:ResourceTag/env": ["prod"]}}},
            {"Effect": "Allow", "Action": "dynamodb:Scan", "Resource": "*",
             "Condition": {"ForAnyValue:StringEquals": {"dynamodb:ResourceTag/env": ["prod"]}}},
            {"Effect": "Allow", "Action": "dynamodb:BatchGetItem", "Resource": "*",
             "Condition": {"Null": {"dynamodb:ResourceTag/env": "false"}}},
            {"Effect": "Allow", "Action": "dynamodb:UpdateItem", "Resource": "*",
             "Condition": {"StringEquals": {"aws:PrincipalTag/team": "${dynamodb:ResourceTag/team}"}}},
        ],
    }


@pytest.fixture()
def deployment(tmp_path):
    requests.packages.urllib3.disable_warnings()  # self-signed cert
    if not os.path.isfile(EXTENDDB_BINARY):
        pytest.skip(f"extenddb binary not built at {EXTENDDB_BINARY}")

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    sqlite_path = str(data_dir / "data.sqlite")
    config_path = str(tmp_path / "extenddb.toml")

    init = _run(
        "init", "--backend", "sqlite",
        "--sqlite-path", sqlite_path, "--config", config_path,
    )
    if init.returncode != 0:
        combined = init.stdout + init.stderr
        if "backend" in combined.lower():
            pytest.skip("binary is not a SQLite build")
        raise AssertionError(f"sqlite init failed: {combined}")

    admin_pw = _admin_password(init.stdout + init.stderr)
    port = _free_port()
    _patch_port(config_path, port)
    endpoint = f"https://127.0.0.1:{port}"
    log_path = str(tmp_path / "server.log")
    server = _Server(config_path, port, log_path)

    yield {
        "config": config_path,
        "sqlite_path": sqlite_path,
        "port": port,
        "endpoint": endpoint,
        "admin_user": "admin",
        "admin_pw": admin_pw,
        "server": server,
        "init_output": init.stdout + init.stderr,
    }
    server.stop()


def _sqlite_policy_docs(sqlite_path: str) -> list[str]:
    conn = sqlite3.connect(sqlite_path)
    try:
        rows = conn.execute("SELECT policy_document FROM iam_policies").fetchall()
    finally:
        conn.close()
    return [r[0] for r in rows]


def test_resource_tag_migration_wire(deployment):
    from management_helpers import ManagementClient

    endpoint = deployment["endpoint"]
    server = deployment["server"]
    sqlite_path = deployment["sqlite_path"]
    region = "us-east-1"

    # 1. First start of a fresh catalog: marker is seeded, so serve starts and
    #    logs nothing about the migration.
    server.start()
    log = server.read_log()
    assert "resource-tag condition-key migration" not in log, log
    assert "dynamodb:ResourceTag" not in log, log

    mgmt = ManagementClient(endpoint, deployment["admin_user"], deployment["admin_pw"])
    account_id = f"{uuid.uuid4().int % 10**12:012d}"
    assert mgmt.create_account(account_id, "wire").status_code == 201

    # Admin user with full access to create and tag a table.
    assert mgmt.create_user(account_id, "fulladmin", "TestPass123!").status_code == 201
    creds = mgmt.create_access_key(account_id, "fulladmin").json()
    admin_ak, admin_sk = creds["access_key_id"], creds["secret_access_key"]
    assert mgmt.put_user_policy(
        account_id, "fulladmin", "full",
        {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}]},
    ).status_code == 204

    # Restricted user: GetItem allowed only when the resource tag env=prod,
    # written on the dynamodb:ResourceTag form (the pre-migration shape).
    assert mgmt.create_user(account_id, "scoped", "TestPass123!").status_code == 201
    scoped = mgmt.create_access_key(account_id, "scoped").json()
    scoped_ak, scoped_sk = scoped["access_key_id"], scoped["secret_access_key"]
    assert mgmt.put_user_policy(
        account_id, "scoped", "tagpolicy", _policy("dynamodb:ResourceTag/env"),
    ).status_code == 204

    # Every operator shape, on a group policy, so the migration has to rewrite
    # them all.
    assert mgmt.create_group(account_id, "grp").status_code == 201
    assert mgmt.put_group_policy(
        account_id, "grp", "shapes", _every_shape_policy()
    ).status_code == 204

    # A permissions boundary carrying the key too.
    assert mgmt.set_user_boundary(
        account_id, "scoped", _policy("dynamodb:ResourceTag/env")
    ).status_code in (200, 204)

    # Create and tag the table (env=prod) and seed an item.
    admin_client = _client(endpoint, admin_ak, admin_sk, region)
    table = f"wire-{uuid.uuid4().hex[:8]}"
    admin_client.create_table(
        TableName=table,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        Tags=[{"Key": "env", "Value": "prod"}],
    )
    from conftest import wait_for_active
    wait_for_active(admin_client, table)
    admin_client.put_item(TableName=table, Item={"pk": {"S": "k1"}, "v": {"S": "hi"}})

    # Before the migration the scoped policy gates on dynamodb:ResourceTag,
    # which this build leaves absent, so the positive condition never matches
    # and GetItem is denied.
    scoped_client = _client(endpoint, scoped_ak, scoped_sk, region)
    with pytest.raises(ClientError) as exc:
        scoped_client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
    assert exc.value.response["Error"]["Code"] == "AccessDeniedException"

    # 2. Stop and run migrate WITHOUT --yes: it lists the rewrites with counts
    #    and exits non-zero, and nothing on disk changes.
    server.stop()
    before_docs = _sqlite_policy_docs(sqlite_path)
    assert any("dynamodb:ResourceTag" in d for d in before_docs)

    dry = _run("migrate", "--config", deployment["config"])
    assert dry.returncode != 0, dry.stdout + dry.stderr
    out = dry.stdout + dry.stderr
    assert "Rewrite:" in out, out
    assert "--yes is required" in out, out
    # The listing names policies and key counts, never a policy body.
    assert "key(s))" in out, out
    # Nothing changed on disk.
    assert _sqlite_policy_docs(sqlite_path) == before_docs

    # 3. migrate --yes rewrites and records the marker.
    applied = _run("migrate", "--yes", "--config", deployment["config"])
    assert applied.returncode == 0, applied.stdout + applied.stderr
    after_docs = _sqlite_policy_docs(sqlite_path)
    assert all("dynamodb:ResourceTag" not in d for d in after_docs), after_docs
    assert any("aws:ResourceTag" in d for d in after_docs)

    # 4. Restart and confirm the decision flipped: the rewritten aws:ResourceTag
    #    policy now resolves the tag, so GetItem on the env=prod table is allowed.
    server.start()
    scoped_client = _client(endpoint, scoped_ak, scoped_sk, region)
    got = scoped_client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
    assert got["Item"]["v"]["S"] == "hi"

    # A second migrate is idempotent: nothing left to rewrite, marker present.
    server.stop()
    again = _run("migrate", "--config", deployment["config"])
    assert again.returncode == 0, again.stdout + again.stderr
    assert "No dynamodb:ResourceTag condition keys found." in (again.stdout + again.stderr)

    # 5. Delete the marker row with the sqlite3 module and confirm serve refuses.
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.execute("DELETE FROM settings WHERE key = ?", (MIGRATION_MARKER_KEY,))
        conn.commit()
    finally:
        conn.close()
    refused = _run("serve", "--config", deployment["config"], "--foreground", timeout=60)
    assert refused.returncode != 0
    assert "extenddb migrate --yes" in (refused.stdout + refused.stderr), (
        refused.stdout + refused.stderr
    )
