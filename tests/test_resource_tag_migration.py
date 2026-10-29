# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""End-to-end wire test for the resource-tag condition-key migration (SQLite).

Drives a real SQLite deployment through its lifecycle on its own data
directory and port, independent of the server the integration job already
started:

  1. init and serve; the first start of a fresh catalog logs no migration
     line.
  2. store an inline user policy, a group policy and a permissions boundary
     using every operator shape of dynamodb:ResourceTag, plus a conditional
     allow used for the decision check, through the management API.
  3. stop, and make the catalog read as one a release before 1.0 left: an
     empty migration ledger, a dotted catalog_version and the old marker row.
  4. run `migrate` without --yes: it lists the pending migrations, the
     condition-key rewrite included, and exits 3, and nothing on disk changes
     (read back with the sqlite3 module).
  5. run `migrate --yes`: it names each rewritten policy with its key count;
     restart, and confirm a GetItem gated on the resource tag that was denied
     before the migration is allowed after it.
  6. write the marker row again, as a release before 1.0 does, and confirm
     serve refuses with exit 5 naming `--reconverge`, which repairs it.

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

# Exit codes `serve` and `migrate` share.
EXIT_PENDING = 3
EXIT_ATTENTION = 5

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


def _as_released_catalog(sqlite_path: str) -> None:
    """Make a catalog this binary created read as one a release before 1.0
    left: no ledger rows (the runner reads an empty ledger as it reads a
    missing one), a dotted catalog_version, and the marker row every released
    baseline seeded. The runner then applies every migration, the
    condition-key rewrite included, as it does on a released catalog."""
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.execute("DELETE FROM extenddb_migrations")
        conn.execute(
            "UPDATE settings SET value = '0.0.3' WHERE key = 'catalog_version'"
        )
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, 'applied')",
            (MIGRATION_MARKER_KEY,),
        )
        conn.commit()
    finally:
        conn.close()


def _sqlite_ledger_ids(sqlite_path: str) -> list[int]:
    conn = sqlite3.connect(sqlite_path)
    try:
        return [
            r[0]
            for r in conn.execute("SELECT id FROM extenddb_migrations ORDER BY id")
        ]
    finally:
        conn.close()


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

    # 1. The first start of a fresh catalog, which init migrated, logs
    #    nothing about the condition-key migration.
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

    # 2. Stop, and make the catalog read as one a release before 1.0 left.
    server.stop()
    _as_released_catalog(sqlite_path)
    before_docs = _sqlite_policy_docs(sqlite_path)
    assert any("dynamodb:ResourceTag" in d for d in before_docs)

    # 3. migrate WITHOUT --yes lists the pending migrations and exits 3 (work
    #    pending, as distinct from an error), and nothing on disk changes.
    dry = _run("migrate", "--config", deployment["config"])
    out = dry.stdout + dry.stderr
    assert dry.returncode == EXIT_PENDING, out
    assert "2 resource_tag_condition_keys (catalog, breaking)" in out, out
    assert "run again with --yes to apply" in out, out
    assert _sqlite_policy_docs(sqlite_path) == before_docs
    assert _sqlite_ledger_ids(sqlite_path) == []

    # 4. migrate --yes rewrites, naming each policy with its key count and
    #    never a policy body, and deletes the old marker.
    applied = _run("migrate", "--yes", "--config", deployment["config"])
    out = applied.stdout + applied.stderr
    assert applied.returncode == 0, out
    assert "rewrote account" in out and "key(s))" in out, out
    assert "policy tagpolicy" in out and "policy shapes" in out, out
    after_docs = _sqlite_policy_docs(sqlite_path)
    assert all("dynamodb:ResourceTag" not in d for d in after_docs), after_docs
    assert any("aws:ResourceTag" in d for d in after_docs)
    assert _sqlite_marker(sqlite_path) == []

    # 5. Restart and confirm the decision flipped: the rewritten aws:ResourceTag
    #    policy now resolves the tag, so GetItem on the env=prod table is allowed.
    server.start()
    scoped_client = _client(endpoint, scoped_ak, scoped_sk, region)
    got = scoped_client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
    assert got["Item"]["v"]["S"] == "hi"

    # A second migrate finds nothing to apply.
    server.stop()
    again = _run("migrate", "--config", deployment["config"])
    assert again.returncode == 0, again.stdout + again.stderr
    assert "Up to date" in (again.stdout + again.stderr)

    # 6. A release before 1.0 writes the marker row again; serve refuses the
    #    catalog with exit 5 and names --reconverge, which repairs it.
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, 'applied')",
            (MIGRATION_MARKER_KEY,),
        )
        conn.commit()
    finally:
        conn.close()
    refused = _run("serve", "--config", deployment["config"], "--foreground", timeout=60)
    out = refused.stdout + refused.stderr
    assert refused.returncode == EXIT_ATTENTION, out
    assert "--yes --reconverge" in out, out
    repaired = _run("migrate", "--yes", "--reconverge", "--config", deployment["config"])
    assert repaired.returncode == 0, repaired.stdout + repaired.stderr
    assert _sqlite_marker(sqlite_path) == []
    server.start()


def _sqlite_marker(sqlite_path: str) -> list:
    conn = sqlite3.connect(sqlite_path)
    try:
        return conn.execute(
            "SELECT value FROM settings WHERE key = ?", (MIGRATION_MARKER_KEY,)
        ).fetchall()
    finally:
        conn.close()


def _sqlite_policy(sqlite_path: str, policy_name: str) -> dict:
    conn = sqlite3.connect(sqlite_path)
    try:
        row = conn.execute(
            "SELECT policy_document FROM iam_policies WHERE policy_name = ?",
            (policy_name,),
        ).fetchone()
    finally:
        conn.close()
    return json.loads(row[0])


def _sqlite_set_policy(sqlite_path: str, policy_name: str, document: dict) -> None:
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.execute(
            "UPDATE iam_policies SET policy_document = ? WHERE policy_name = ?",
            (json.dumps(document), policy_name),
        )
        conn.commit()
    finally:
        conn.close()


def test_resource_tag_migration_collisions(deployment):
    """Both resource-tag forms on one tag key in one operator block.

    Same values: both forms resolved from the same table tags before the
    upgrade, so the two conditions are one condition. migrate drops the
    dynamodb: key and the Deny keeps applying.

    Different values: the two cannot share one JSON key. migrate finds the
    collision before it writes anything and exits 5, and serve refuses the
    catalog with exit 5 until the policy is fixed and migrate is run again.
    Applying the migration here would change the decision: under
    StringNotEquals the dynamodb: half, now absent, always matches.
    """
    from management_helpers import ManagementClient

    endpoint = deployment["endpoint"]
    server = deployment["server"]
    sqlite_path = deployment["sqlite_path"]
    region = "us-east-1"

    server.start()
    mgmt = ManagementClient(endpoint, deployment["admin_user"], deployment["admin_pw"])
    account_id = f"{uuid.uuid4().int % 10**12:012d}"
    assert mgmt.create_account(account_id, "collide").status_code == 201

    assert mgmt.create_user(account_id, "fulladmin", "TestPass123!").status_code == 201
    creds = mgmt.create_access_key(account_id, "fulladmin").json()
    assert mgmt.put_user_policy(
        account_id, "fulladmin", "full",
        {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}]},
    ).status_code == 204

    # Same-value duplicate on a Deny.
    assert mgmt.create_user(account_id, "hedged", "TestPass123!").status_code == 201
    hedged = mgmt.create_access_key(account_id, "hedged").json()
    assert mgmt.put_user_policy(account_id, "hedged", "hedged-same", {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*"},
            {"Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
             "Condition": {"StringEquals": {
                 "aws:ResourceTag/env": "prod",
                 "dynamodb:ResourceTag/env": "prod",
             }}},
        ],
    }).status_code == 204

    # Different values, which cannot be merged.
    assert mgmt.create_user(account_id, "split", "TestPass123!").status_code == 201
    split_doc = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
             "Condition": {"StringNotEquals": {
                 "aws:ResourceTag/env": "dev",
                 "dynamodb:ResourceTag/env": "test",
             }}},
        ],
    }
    assert mgmt.put_user_policy(account_id, "split", "hedged-split", split_doc).status_code == 204

    admin_client = _client(endpoint, creds["access_key_id"], creds["secret_access_key"], region)
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
    admin_client.put_item(TableName=table, Item={"pk": {"S": "k1"}})
    server.stop()

    # A catalog a release before 1.0 left, with both policies stored on it.
    _as_released_catalog(sqlite_path)
    same_before = _sqlite_policy(sqlite_path, "hedged-same")

    # The collision is found before anything is written: no ledger rows, and
    # both policies as they were, the mergeable one included.
    applied = _run("migrate", "--yes", "--config", deployment["config"])
    out = applied.stdout + applied.stderr
    assert applied.returncode == EXIT_ATTENTION, out
    assert "policy hedged-split" in out, out
    assert "policy hedged-same" not in out, out
    assert _sqlite_ledger_ids(sqlite_path) == []
    assert _sqlite_policy(sqlite_path, "hedged-split") == split_doc
    assert _sqlite_policy(sqlite_path, "hedged-same") == same_before

    refused = _run("serve", "--config", deployment["config"], "--foreground", timeout=60)
    out = refused.stdout + refused.stderr
    assert refused.returncode == EXIT_ATTENTION, out
    assert "policy hedged-split" in out, out
    assert "extenddb migrate --config" in out, out

    # Fix the different-value policy by hand (keep the aws: form) and run
    # migrate again: it applies, and the same-value policy keeps only the
    # aws: key.
    fixed = json.loads(json.dumps(split_doc))
    del fixed["Statement"][0]["Condition"]["StringNotEquals"]["dynamodb:ResourceTag/env"]
    _sqlite_set_policy(sqlite_path, "hedged-split", fixed)
    again = _run("migrate", "--yes", "--config", deployment["config"])
    assert again.returncode == 0, again.stdout + again.stderr
    assert _sqlite_ledger_ids(sqlite_path) != []
    assert _sqlite_marker(sqlite_path) == []
    same = _sqlite_policy(sqlite_path, "hedged-same")
    assert same["Statement"][1]["Condition"] == {
        "StringEquals": {"aws:ResourceTag/env": "prod"}
    }

    # The same-value Deny still applies after the upgrade.
    server.start()
    hedged_client = _client(
        endpoint, hedged["access_key_id"], hedged["secret_access_key"], region
    )
    with pytest.raises(ClientError) as exc:
        hedged_client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
    assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
