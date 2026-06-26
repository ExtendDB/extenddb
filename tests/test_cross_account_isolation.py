# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Cross-account isolation tests — verify data plane and console isolation.

Two accounts, two users (one per account), each with full DynamoDB access.
Verifies that tables, items, and console views are isolated between accounts.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars
  - `extenddb init` has been run (encryption key + admin user exist)

Run:
  EXTENDDB_TEST_ENDPOINT=http://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin \\
  EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_cross_account_isolation.py -v

REQ-TEST-001, REQ-AUTH-002
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import boto3
import pytest
import requests
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_deleted
from management_helpers import ManagementClient
def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: Cross-account tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    return endpoint, admin_user, admin_pass
def _make_dynamodb_client(endpoint_url: str, access_key: str, secret_key: str,
                          region: str) -> Any:
    kwargs: dict = dict(
        service_name="dynamodb",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    # D4: Self-signed certs from ``extenddb init`` — disable SSL verification.
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)
def _full_access_policy() -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "dynamodb:*",
            "Resource": "*",
        }],
    }
@pytest.fixture(scope="module")
def auth_env():
    return _require_auth_env()
@pytest.fixture(scope="module")
def mgmt(auth_env) -> ManagementClient:
    endpoint, admin_user, admin_pass = auth_env
    return ManagementClient(endpoint, admin_user, admin_pass)
@pytest.fixture(scope="module")
def region() -> str:
    return os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
@pytest.fixture(scope="module")
def alice_env(auth_env, mgmt, region):
    """Create account + user 'alice' with full DynamoDB access. Return (client, account_id)."""
    endpoint = auth_env[0]
    acct_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct_id, f"alice-acct-{acct_id}")
    assert resp.status_code == 201, resp.text

    resp = mgmt.create_user(acct_id, "alice", "AlicePass123!")
    assert resp.status_code == 201, resp.text
    resp = mgmt.create_access_key(acct_id, "alice")
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    resp = mgmt.put_user_policy(acct_id, "alice", "full", _full_access_policy())
    assert resp.status_code == 204, resp.text

    client = _make_dynamodb_client(endpoint, creds["access_key_id"],
                                   creds["secret_access_key"], region)
    yield client, acct_id

    mgmt.delete_user(acct_id, "alice")
    mgmt.delete_account(acct_id)
@pytest.fixture(scope="module")
def bob_env(auth_env, mgmt, region):
    """Create account + user 'bob' with full DynamoDB access. Return (client, account_id)."""
    endpoint = auth_env[0]
    acct_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct_id, f"bob-acct-{acct_id}")
    assert resp.status_code == 201, resp.text

    resp = mgmt.create_user(acct_id, "bob", "BobPass456!")
    assert resp.status_code == 201, resp.text
    resp = mgmt.create_access_key(acct_id, "bob")
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    resp = mgmt.put_user_policy(acct_id, "bob", "full", _full_access_policy())
    assert resp.status_code == 204, resp.text

    client = _make_dynamodb_client(endpoint, creds["access_key_id"],
                                   creds["secret_access_key"], region)
    yield client, acct_id

    mgmt.delete_user(acct_id, "bob")
    mgmt.delete_account(acct_id)
# ---------------------------------------------------------------------------
# Data Plane Isolation
# ---------------------------------------------------------------------------

class TestDataPlaneIsolation:
    """Tables and items are isolated between accounts."""

    @pytest.fixture(autouse=True)
    def setup(self, alice_env, bob_env):
        self.alice, self.alice_acct = alice_env
        self.bob, self.bob_acct = bob_env

    def test_alice_tables_invisible_to_bob(self):
        """Tables created by alice do not appear in bob's ListTables."""
        table = f"iso-{uuid.uuid4().hex[:8]}"
        try:
            self.alice.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(self.alice, table)

            bob_tables = self.bob.list_tables()["TableNames"]
            assert table not in bob_tables
        finally:
            try:
                self.alice.delete_table(TableName=table)
            except Exception:
                pass
            else:
                wait_for_deleted(self.alice, table)

    def test_same_name_tables_coexist_independently(self):
        """Alice and bob can both create a table with the same name."""
        table = f"shared-{uuid.uuid4().hex[:8]}"
        try:
            self.alice.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(self.alice, table)

            self.bob.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(self.bob, table)

            # Both should see the table in their own listing.
            assert table in self.alice.list_tables()["TableNames"]
            assert table in self.bob.list_tables()["TableNames"]

            # Write different data to each.
            self.alice.put_item(
                TableName=table,
                Item={"pk": {"S": "key1"}, "owner": {"S": "alice"}},
            )
            self.bob.put_item(
                TableName=table,
                Item={"pk": {"S": "key1"}, "owner": {"S": "bob"}},
            )

            # Each sees their own data.
            alice_item = self.alice.get_item(
                TableName=table, Key={"pk": {"S": "key1"}}
            )["Item"]
            assert alice_item["owner"]["S"] == "alice"

            bob_item = self.bob.get_item(
                TableName=table, Key={"pk": {"S": "key1"}}
            )["Item"]
            assert bob_item["owner"]["S"] == "bob"
        finally:
            for client in (self.alice, self.bob):
                try:
                    client.delete_table(TableName=table)
                except Exception:
                    pass
                else:
                    wait_for_deleted(client, table)

    def test_alice_cannot_read_bob_items(self):
        """Alice cannot read items from bob's table (table doesn't exist in her namespace)."""
        table = f"bob-only-{uuid.uuid4().hex[:8]}"
        try:
            self.bob.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(self.bob, table)
            self.bob.put_item(
                TableName=table,
                Item={"pk": {"S": "secret"}, "data": {"S": "bob-private"}},
            )

            # Alice tries to read from the same table name — should fail
            # because the table doesn't exist in her account.
            with pytest.raises(ClientError) as exc_info:
                self.alice.get_item(
                    TableName=table, Key={"pk": {"S": "secret"}}
                )
            assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            try:
                self.bob.delete_table(TableName=table)
            except Exception:
                pass
            else:
                wait_for_deleted(self.bob, table)

    def test_alice_cannot_write_to_bob_table(self):
        """Alice cannot write to a table that only exists in bob's account."""
        table = f"bob-write-{uuid.uuid4().hex[:8]}"
        try:
            self.bob.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(self.bob, table)

            with pytest.raises(ClientError) as exc_info:
                self.alice.put_item(
                    TableName=table,
                    Item={"pk": {"S": "intruder"}, "data": {"S": "hacked"}},
                )
            assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            try:
                self.bob.delete_table(TableName=table)
            except Exception:
                pass
            else:
                wait_for_deleted(self.bob, table)

    def test_delete_in_one_account_does_not_affect_other(self):
        """Deleting a same-name table in alice's account doesn't affect bob's."""
        table = f"del-iso-{uuid.uuid4().hex[:8]}"
        try:
            for client in (self.alice, self.bob):
                client.create_table(
                    TableName=table,
                    AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                    KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                    BillingMode="PAY_PER_REQUEST",
                )
                wait_for_active(client, table)

            self.bob.put_item(
                TableName=table,
                Item={"pk": {"S": "k1"}, "v": {"S": "bob-data"}},
            )

            # Alice deletes her copy.
            self.alice.delete_table(TableName=table)
            wait_for_deleted(self.alice, table)

            # Bob's table and data should be unaffected.
            assert table in self.bob.list_tables()["TableNames"]
            item = self.bob.get_item(
                TableName=table, Key={"pk": {"S": "k1"}}
            )["Item"]
            assert item["v"]["S"] == "bob-data"
        finally:
            try:
                self.bob.delete_table(TableName=table)
            except Exception:
                pass
            else:
                wait_for_deleted(self.bob, table)
# ---------------------------------------------------------------------------
# Console Isolation
# ---------------------------------------------------------------------------

class TestConsoleIsolation:
    """IAM users in different accounts see only their own account's entities."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, alice_env, bob_env):
        self.endpoint = auth_env[0]
        self.alice_acct = alice_env[1]
        self.bob_acct = bob_env[1]

    def _console_login(self, account_id: str, user_name: str,
                       password: str) -> requests.Session:
        """Login to console as IAM user, return session with cookie."""
        session = requests.Session()
        # D4: Self-signed certs from ``extenddb init`` — disable SSL verification.
        if self.endpoint.startswith("https://"):
            session.verify = False
        resp = session.post(
            f"{self.endpoint}/console/login",
            data={"username": f"{account_id}/{user_name}", "password": password},
            allow_redirects=False,
            timeout=30,
        )
        assert resp.status_code == 303, f"Console login failed: {resp.status_code}"
        return session

    def test_alice_console_does_not_show_bob_account(self):
        """Alice's console session does not list bob's account."""
        session = self._console_login(self.alice_acct, "alice", "AlicePass123!")
        resp = session.get(
            f"{self.endpoint}/console/accounts",
            allow_redirects=False,
            timeout=30,
        )
        assert resp.status_code == 200
        assert self.alice_acct in resp.text
        assert self.bob_acct not in resp.text

    def test_bob_console_does_not_show_alice_account(self):
        """Bob's console session does not list alice's account."""
        session = self._console_login(self.bob_acct, "bob", "BobPass456!")
        resp = session.get(
            f"{self.endpoint}/console/accounts",
            allow_redirects=False,
            timeout=30,
        )
        assert resp.status_code == 200
        assert self.bob_acct in resp.text
        assert self.alice_acct not in resp.text
